## YAML Transformer Constructs

A Yaml Transformer definition can have following constructs at root:
  - inputs: Inputs sources to be transformed.
  - transformations: Transformations definitions.
  - outputs: Output targets (only defined for end-to-end mode).

#### 1. inputs

- End-to-End Transformation file:
The inputs sources for transformation can be provided as follows:
 ```yaml
 inputs:
       - {type: hive, alias: product, db: dim, table: product}
       - {type: hive, alias: date, db: dim, table: date}
 ```
Following input/source types are supported:

Source | Definition | Description
-------|----------- |------------
hive   | {type: hive, alias: <dataframe_alias>, db: <hive_database>, table: <hive_table>} |
json   | {type: json, alias: <dataframe_alias>, path: <hdfs_file_path>, is_streaming: true/false, schema_file: <file_path>, options: {opt1: val1}} | options: provide json options (optional), is_streaming: marks as streaming source (optional, defaults to false), schema_file: path to schema file (optional)
csv   | {type: csv, alias: <dataframe_alias>, path: <hdfs_file_path>, is_streaming: true/false, schema_file: <file_path>, options: {opt1: val1}} | options: provide csv options (optional), is_streaming: marks as streaming source (optional, defaults to false), schema_file: path to schema file (optional)
parquet   | {type: parquet, alias: <dataframe_alias>, path: <hdfs_file_path>, is_streaming: true/false, schema_file: <file_path>, options: {opt1: val1}} | options: parquet options (optional), is_streaming: marks as streaming source (optional, defaults to false), schema_file: path to schema file (optional)
orc   | {type: orc, alias: <dataframe_alias>, path: <hdfs_file_path>, is_streaming: true/false, schema_file: <file_path>, options: {opt1: val1}} | options: provide orc options (optional), is_streaming: marks as streaming source (optional, defaults to false), schema_file: path to schema file (optional)
text   | {type: text, alias: <dataframe_alias>, path: <hdfs_file_path>, is_streaming: true/false, schema_file: <file_path>, options: {opt1: val1}} | options: provide text options (optional), is_streaming: marks as streaming source (optional, defaults to false), schema_file: path to schema file (optional)
jdbc | {type: jdbc, alias: <dataframe_alias>, url: <jdbc_url>, table: <jdbc_table>, options: {opt1: val1, ..}} | options: provide jdbc options (optional)
bigquery |  {type: bigquery, alias: <dataframe_alias>, db: <bigquery_dataset>, table: <bigquery_table>, project_id: <id>} | project_id is optional
custom | {type: custom, alias: <dataframe_alias>, format: <custom_source_format>, is_streaming: true/false, schema_file: <file_path>, options: {opt1: val1}} | options: provide custom source options (optional), is_streaming: marks as streaming source, if supported by custom source (optional, defaults to false), schema_file: path to schema file (optional)


- Intermediate Transformations:
  For this mode, the 'inputs' section just takes the logical aliases for dataframes that will be passed to yaml transformer.
  ```yaml
    inputs: [transactions, product, date]
  ```

#### 2. outputs

 The 'outputs' construct is only supported for end-to-end mode. Following is a sample definition of 'outputs' section:
 ```yaml
 outputs:
     - {type: csv, input: transactions, mode: overwrite, path: /tmp/result.csv}
     - {type: json, input: product, mode: overwrite, path: /tmp/result1.csv}
 ```
 Supported write modes: overwrite, append, error_if_exits, ignore
 Following output/target types are supported:

 Source | Definition | Description
 -------|----------- |------------
 hive   | {type: hive, input: <transformation_alias>, mode: <write_mode>, db: <hive_database>, table: <hive_table>, save_as_table: <true/false>, partition_by: [col1. col2, ..], bucket_by: {num_buckets: <int>, columns: [col1, col2, ..]}} | save_as_table: if set, writes the data using saveAsTable api.(optional, defaults to false), partition_by and bucket_by are optional
 json   | {type: json, input: <transformation_alias>, mode: <write_mode>, path: <hdfs_output_path>, options: {opt1: val1}, partition_by: [col1. col2, ..], bucket_by: {num_buckets: <int>, columns: [col1, col2, ..]}} | options: provide json options (optional), partition_by and bucket_by are optional
 csv   | {type: csv, input: <transformation_alias>, mode: <write_mode>, path: <hdfs_output_path>, options: {opt1: val1}, partition_by: [col1. col2, ..], bucket_by: {num_buckets: <int>, columns: [col1, col2, ..]}} | options: provide csv options (optional), partition_by and bucket_by are optional
 orc   | {type: orc, input: <transformation_alias>, mode: <write_mode>, path: <hdfs_output_path>, options: {opt1: val1}, partition_by: [col1. col2, ..], bucket_by: {num_buckets: <int>, columns: [col1, col2, ..]}} | options: provide orc options (optional), partition_by and bucket_by are optional
 parquet  | {type: parquet, input: <transformation_alias>, mode: <write_mode>, path: <hdfs_output_path>, options: {opt1: val1}, partition_by: [col1. col2, ..], bucket_by: {num_buckets: <int>, columns: [col1, col2, ..]}} | options: provide parquet options (optional), partition_by and bucket_by are optional
 text   | {type: text, input: <transformation_alias>, mode: <write_mode>, path: <hdfs_output_path>, options: {opt1: val1}, partition_by: [col1. col2, ..], bucket_by: {num_buckets: <int>, columns: [col1, col2, ..]}} | options: provide text options (optional), partition_by and bucket_by are optional
 jdbc  | {type: jdbc, input: <transformation_alias>, mode: <write_mode>, url: <jbbc_url>, table: <jdbc_table>, options: {opt1: val1} }|  options: provide jdbc options (optional)
 bigquery  | {type: bigquery, input: <transformation_alias>, mode: <write_mode>, db: <bigquery_dataset>, table: <bigquery_table>, project_id: <id>} | project id is optional
 custom  |  {type: custom, input: <transformation_alias>, format: <output_format>, mode: <write_mode_batch>, is_streaming: <true/false>, output_mode: <write_mode_streaming>, options: {opt1: val1}, partition_by: [col1. col2, ..], bucket_by: {num_buckets: <int>, columns: [col1, col2, ..]}} | options: provide custom target options (optional), partition_by and bucket_by are optional. is_streaming is optional, defaults to false. If set, provide output_mode for the type of streaming write mode
 count | {type: count, input: <transformation_alias> }| count the records in the provided transformation alias
 show | {type: show, num_records: <int>} | num_records is optional, defaults to 20

#### 3. transformations`
The 'transformations' section if defined as follows:

```yaml
transformations :
    - input: input_alias  // Input alias on which transformations will performed. Can be from 'inputs' sections or previous transformation
      alias: a1 // alias for this transformation
      aliases: [a1, a2] // aliases if the transformation returns multiple aliases. NOTE: Use either alias/aliases depending on the output scenario.
      persist: memory_only  // Persistence level if transformation is to be persisted. e.g. MEMORY_ONLY, MEMORY_AND_DISK (optional)
      actions: // define multiple transformation actions for this input.
        - {type: <transformation>, <transformation_options>}
        - {type: <transformation>, <transformation_options>}
        - {type: <multi_out_transformation>, <transformation_options>} // If the action is multi-output action, it should be the LAST transformation step/action
    - input: input_alias1
      alias: a3
      sql: select * from input_alias1 // SQL can also be provided for Simple transformations. Either 'sql' or 'actions' can be provided.
    - input: input_alias2
      persist: true
      actions:
        - {type: <transformation>, <transformation_options (can_refer_previous_transform: input_alias1)>}
        ....
    ....
```

Following single output transformation actions are supported:

Transformation | Definition | Description
---------------|------------|------------
Select | {type: select, columns: [col1, col2, ..]} | Select the columns
Select with Reorder | {type: select_reorder, alias: <reordering_alias> } | Reorders the dataframe columns based on the columns in another dataframe alias.
Select with Alias | { type: select_alias, include_columns: [col1, col2 ..], exclude_cols: [col1, col2 ..] } | Select columns from a dataframe which was joined using aliased dataframes. Useful if you want to get a structure of pre-joined dataframe of provided alias and some join columns from opposite side of join. Columns from provided aliased side can be excluded using skip_columns (optional).
Drop Columns |  {type: drop, columns: [col1, col2, ...]} |
Filter | {type: filter, condition: filter_condition} | Filter transformation
Rename  | {type: rename, list: {col1: new_col1, col2: new_col2, ..}} | Rename columns. list: list of existing name and new name
Rename All  | {type: rename_all, append: <string_to_append_to_all_columns>, prefix: <true/false>, columns: [col1, col2] , pattern: <regex_pattern>} | Rename all/selected/pattern matching columns by appending provided string. columns & pattern are optional, if not provided, all columns are renamed. prefix is optional, defaults to false i.e. renames in suffix mode.
Column Transform |{type: transform, list: {col1: func(<any_col>), col2: func(<any_col1>, <any_expr>), ..} }| list of column name and functions applied to column
Column Transform(Bulk)| {type: transform_all, function: <single_arg_function>, suffix: <suffix_to_append>, columns: [col1, col2, expr1, expr2] } | **suffix** is optional, if not provided the columns will be replaced by transformed column else new column with appended suffix will be generated. **columns** is optional, if not provided the function will be applied on all the columns of the input
Column Transform (Multi Arg Function) |{type: transform_multi_arg, function: <multi_arg_function> , list: {col1: [<any_col1>, <any_col2>], col2: [<any_col1>, <any_expr>], ..} }| list of argument list (columns) for the multi argument function
Limit | {type: limit, size: <int> } | Limit the records
Distinct | {type: distinct} |
Drop Duplicates |  {type: drop_duplicates, columns: [col1, col2, ...]} | Drop duplicate row using specified columns
Unstruct | {type: unstruct, column: col1, col2} | Flattens the struct columns.
Join | {type: join, columns: [col1, col2, ..] , condition: x.col1 = y.col2, join_type: <join_type>, with: <join_dataframe>, broadcast_hint: <left/right>} | Either columns or condition should be provided; join_types: inner, left, right, full; broadcast_hint: left/right (optional)
Group By |{type: group_by, columns: [col1, col2, ..], exprs: group_expression} |
Order By |{type: order_by, columns: [col1:desc, col2, ..]} | to sort in descending order, append column by ':desc'
Union | {type: union, with: [dataset1, dataset2, ..]} |
Intersect | {type: intersect, with: [dataset1, dataset2, ..]} |
Except | {type: except, with: [dataset1, dataset2, ..]} |
Repartition | {type: repartition, columns: [col1, col2, ...], num_partitions: <int>} | columns are optional, num_partitions is optional, defaults to 200
Coalesce | {type: coalesce, num_partitions: <int> | 
Sequence | {type: sequence, sk_source: source_alias, sk_column: source_key_column} | Generates Sequential numbers. max 'sk_column' value plus 1 will used as start index for sequence generation.
SCD | {type: scd, dim_table: table_name, sk_column: sk_key, natural_keys: [nk1, nk2, ..], derived_columns: [derived_col1, ..], meta_columns: [update_date, ..], incremental_load: true(default)/false } | Perform SCD on the dimension table, the output will be fused input & dimension records with 'INSERT', 'UPDATE', 'NCD' or 'INVALID' scd_status. Users can update this dataset according to required SCD type.
NA  | {type: na, default_numeric: <default_numeric_value_for_nulls>, default_string: <default_string_value_for_nulls>, columns: [col1, col2, ..]} | Assigns default numeric/string values to NULLs VALUES for provided columns
Call Yaml | {type: yaml, yaml_file: <yaml_file_name_to_call>, substitutions: [s1, s2, {k1: v1, k2: v2}], input_aliases: [i1, i2, i3..], output_alias: output} | Calls another yaml and gets the transformation output alias. The substitutions are optional. If provided the substitution should contain valid yaml objects and should correspond to valid type in the yaml template.
Named Transformation | {type: named_transform , name: <registered_transformation_name>, inputs: [t1, t2]} | Calls registered transformation. If transformation takes more than one input transformations , than provide them in required order in 'inputs'
Data Quality Check | {type: dq_check, dq_function: <function>, id: <unique_dq_id>, columns: [col1, col2, ..] } | Checks the columns against provided condition. Outputs function check results for each column (true/false) and also a column with overall result for the condition (names of columns failing the condition)
Collect | {type: collect, alias: <alias_to_collect>, column: <column_to_collect>, placeholder: <name_for_collect_output>} | Collects the value(s) from provided column of alias and assigns it to a placeholder reference. This reference can be used at other places for in any SQL expression for substitution. Collect does not change the input but just causes a side-effect. 
Watermark | {type: watermark, event_time: <event_column>, delay_threshold: <threshold time string> } | *For streaming mode; use to specify the watermark

Following multi-output transformation actions are supported:

Transformation | Definition | Description
---------------|------------|------------
Partition | {type: partition , condition: <boolean_expression> } | outputs are two transformations: First transformation will satisfy the passed boolean condition, other one will be an opposite set.
Router |  {type: router , conditions: [<boolean_expr_1> , <boolean_expr_2>, ...]} | outputs transformation matching the passed conditions and another default set which negates all the conditions provided. Note: you need to provide the alias for the default set if needed.