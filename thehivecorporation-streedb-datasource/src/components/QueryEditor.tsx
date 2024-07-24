import React, { ChangeEvent } from 'react';
import { InlineField, Input, Stack } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery } from '../types';

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onPrimaryIdxTextChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, primaryIdx: event.target.value });
  };
  const onSecondaryIdxTextChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, secondaryIdx: event.target.value });
  };


  const { primaryIdx } = query;
  const { secondaryIdx } = query;

  return (
    <Stack gap={0}>
      <InlineField label="Primary Index" labelWidth={16} tooltip="Primary Index">
        <Input
          id="query-editor-primary-idx"
          onChange={onPrimaryIdxTextChange}
          value={primaryIdx || ''}
          placeholder="instance1"
        />
      </InlineField>
      <InlineField label="Secondary Index" labelWidth={18} tooltip="Secondary Index">
        <Input
          id="query-editor-secondary-idx"
          onChange={onSecondaryIdxTextChange}
          value={secondaryIdx || ''}
          placeholder="cpu"
        />
      </InlineField>
    </Stack>
  );
}
