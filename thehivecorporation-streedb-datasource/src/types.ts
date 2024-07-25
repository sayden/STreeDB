import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export interface MyQuery extends DataQuery {
  queryText?: string;
  primaryIdx?: string;
  secondaryIdx?: string;
  path?: string;
}

export const DEFAULT_QUERY: Partial<MyQuery> = {
};

export interface DataSourceResponse {
  MetricCategory: string;
  MetricName: string;
  Ts: [number];
  Val: [number];
}

/**
 * These are options configured for each DataSource instance
 */
export interface MyDataSourceOptions extends DataSourceJsonData {
  url?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface MySecureJsonData {
  apiKey?: string;
}
