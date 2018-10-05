## YAML Transformer Constructs

A Yaml Transformer definition can have following constructs at root:
  - inputs : Inputs sources to be transformed.
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
hive   | {type: hive, alias: #dataframe_alias, db: #hive_database, table: #hive_table} |
json   | {type: json, alias: #dataframe_alias, path: #hdfs_file_path, options: {opt1: val1}} | options: provide json options (optional)
csv   | {type: csv, alias: #dataframe_alias, path: #hdfs_file_path, options: {opt1: val1}} | options: provide csv options (optional)
parquet   | {type: parquet, alias: #dataframe_alias, path: #hdfs_file_path, options: {opt1: val1}} | options: parquet options (optional)
orc   | {type: orc, alias: #dataframe_alias, path: #hdfs_file_path, options: {opt1: val1}} | options: provide orc options (optional)
text   | {type: text, alias: #dataframe_alias, path: #hdfs_file_path, options: {opt1: val1}} | options: provide text options (optional)

- Intermediate Transformations:
  For this mode the 'inputs' section just takes the logical aliases for dataframes that will passed to yaml transformer.
  ```yaml
    inputs: ["transactions", "product", "date"]
  ```

#### 2. outputs

 The 'outputs' construct is only supported for end-to-end mode. Following is a sample definition of 'outputs' section:
 ```yaml
 outputs:
     - {type: csv, input: "transactions", mode: overwrite, path: "/tmp/result.csv"}
     - {type: json, input: "product", mode: overwrite, path: "/tmp/result1.csv"}
 ```
 Supported write modes: overwrite, append, error_if_exits, ignore
 Following output/target types are supported:

 Source | Definition | Description
 -------|----------- |------------
 hive   | {type: hive, input: #transformation_alias, mode: #write_mode, db: #hive_database, table: #hive_table} |
 json   | {type: json, input: #transformation_alias, mode: #write_mode, path: #hdfs_output_path, options: {opt1: val1}} | options: provide json options (optional)
 csv   | {type: csv, input: #transformation_alias, mode: #write_mode, path: #hdfs_output_path, options: {opt1: val1}} | options: provide csv options (optional)
 orc   | {type: orc, input: #transformation_alias, mode: #write_mode, path: #hdfs_output_path, options: {opt1: val1}} | options: provide orc options (optional)
 parquet  | {type: parquet, input: #transformation_alias, mode: #write_mode, path: #hdfs_output_path, options: {opt1: val1}} | options: provide parquet options (optional)
 text   | {type: text, input: #transformation_alias, mode: #write_mode, path: #hdfs_output_path, options: {opt1: val1}} | options: provide text options (optional)


#### 3. transformations
The 'transformations' section if defined as follows:

```yaml
transformations :
    - input: input_alias  // Input alias on which transformations will performed. Can be from 'inputs' sections or previous transformation
      alias: alias_for_this_transformation  //  (optional) if not provided, input alias will be replaced with this transformation
      persist: true  // Persistence level if transformation is to be persisted. e.g. MEMORY_ONLY, MEMORY_AND_DISK (optional)
      actions: // define multiple transformation actions for this input
        - {type: <transformation>, <transformation_options>}
        - {type: <transformation>, <transformation_options>}
        ....
    - input: input_alias1
      sql: "select * from input_alias1" // SQL can also be provided for Simple transformations. Either 'sql' or 'actions' can be provided.
    - input: input_alias2
      persist: true
      actions:
        - {type: <transformation>, <transformation_options (can_refer_previous_transform: input_alias1)>}
        ....
    ....
```

Following transformation actions are supported:

Transformation | Definition | Description
---------------|------------|------------
Select | {type: select, columns: [col1, col2, ..]} |
SelectNot | {type: select_not, columns: [col1, col2, ..]} |
Filter | {type: filter, condition: filter_condition} |
Rename  | {type: rename, list: {col1: new_col1, col2: new_col2, ..}} | list: list of existing name and new name
Column Transform |{type: transform, list: {col1: func(<any_col>), col2: func(<any_col>), ..} }| list of column name and functions applied to column
Limit | {type: "limit", size: <int> } |
Distinct | {type: distinct} |
Drop Columns |  {type: drop, columns: ["col1", "col2", ...]} |
Drop Duplicates |  {type: drop_duplicates, columns: ["col1", "col2", ...]} |
Unstruct | {type: unstruct, column: "col1", "col2"} |
Join | {type: join, columns: [col1, col2, ..] , condition: "x.col1 = y.col2", join_type: #join_type, with: #join_dataframe, broadcast_hint: left/right} | Either columns or condition should be provided; join_types: inner, left, right, full; broadcast_hint: left/right (optional)
Group |{type: group_by, columns: [col1, col2, ..], expr: group_expression} |
Order By |{type: order_by, columns: [col1:desc, col2, ..]} | to sort in descending order, append column by ':desc'
Union | {type: union, with: ["dataset1", "dataset2", ..]} |
Intersect | {type: intersect, with: ["dataset1", "dataset2", ..]} |
Except | {type: except, with: ["dataset1", "dataset2", ..]} |
Sequence | {type: sequence, sk_source: source_alias, sk_column: source_key_column} | max 'sk_column' value plus 1 will used as start index for sequence generation
SCD | {type: "scd", dim_table: "table_name", sk_column: "sk_key", natural_keys: ["nk1", "nk2", ..], derived_columns: ["derived_col1", ..], meta_columns: ["update_date", ..], incremental_load: true(default)/false } | Perform SCD on the dimension table, the output will be fused input & dimension records with 'INSERT', 'UPDATE', 'NCD' or 'INVALID' scd_status. Users can update this dataset according to required SCD type.

