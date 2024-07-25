import {
  CoreApp,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  DataFrame,
  FieldType,
} from '@grafana/data';
import { getBackendSrv, isFetchError } from '@grafana/runtime';
import { MyQuery, MyDataSourceOptions, DEFAULT_QUERY, DataSourceResponse } from './types';
import { lastValueFrom } from 'rxjs';
import _ from 'lodash';

export class DataSource extends DataSourceApi<MyQuery, MyDataSourceOptions> {
  baseUrl: string;

  constructor(instanceSettings: DataSourceInstanceSettings<MyDataSourceOptions>) {
    console.log('instanceSettings', instanceSettings);
    super(instanceSettings);
    this.baseUrl = instanceSettings.url!;
  }

  getDefaultQuery(_: CoreApp): Partial<MyQuery> {
    return DEFAULT_QUERY;
  }

  filterQuery(query: MyQuery): boolean {
    // if no query has been provided, prevent the query from being executed
    return !!query.queryText;
  }

  async query(options: DataQueryRequest<MyQuery>): Promise<DataQueryResponse> {
    const { range } = options;
    const from = range!.from.valueOf();
    const to = range!.to.valueOf();


    const data = options.targets.map(async (target) => {
      return this.request(`/api/${target.path}${target.primaryIdx}/${target.secondaryIdx}`, `from=${from}&to=${to}`)
        .then((response) => {
          const frame: DataFrame = {
            name: response.data.MetricName,
            length: response.data.Ts.length,
            refId: target.refId,
            fields: [
              { name: 'Ts', type: FieldType.time, values: response.data.Ts, config: {} },
              { name: 'Val', type: FieldType.number, values: response.data.Val, config: {} },
            ],
          };

          return frame;
        });
    });

    return { data: await Promise.all(data) };
  }

  async request(url: string, params?: string) {
    const response = getBackendSrv().fetch<DataSourceResponse>({
      url: `${this.baseUrl}${url}${params?.length ? `?${params}` : ''}`,
    });
    return lastValueFrom(response);
  }

  /**
   * Checks whether we can connect to the API.
   */
  async testDatasource() {
    const defaultErrorMessage = 'Cannot connect to API';

    try {
      const response = await this.request('/ping');
      if (response.status === 200) {
        return {
          status: 'success',
          message: 'Success',
        };
      } else {
        return {
          status: 'error',
          message: response.statusText ? response.statusText : defaultErrorMessage,
        };
      }
    } catch (err) {
      let message = '';
      if (_.isString(err)) {
        message = err;
      } else if (isFetchError(err)) {
        message = 'Fetch error: ' + (err.statusText ? err.statusText : defaultErrorMessage);
        if (err.data && err.data.error && err.data.error.code) {
          message += ': ' + err.data.error.code + '. ' + err.data.error.message;
        }
      }
      return {
        status: 'error',
        message,
      };
    }
  }
}
