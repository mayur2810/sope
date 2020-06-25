sope-etl
========

### YAML Transformer:

The YAML Transformer reads a yaml file and executes the transformations defined in the file at runtime (during spark-submit).
You will find the transformer useful for developing monotonous and lengthy transformation pipelines by using an external YAML file, instead of writing scala/java files.
Also, the transformer provides high-level abstractions for commonly used processing patterns.
Following are some useful features provided by the transformer:
- Simple: Simple and easy to use constructs with support for SQL.
- Optimizations: Auto persists and pre-partitions persisted data for join optimization.
- Streaming mode: Supports Structured Streaming.
- Template support: Create reusable yaml templates (Call yaml within yaml with support for substitutions).
- Custom Transformations/UDF support: Call custom transformations/udfs written in Scala/Java. Support for dynamic UDFs.
- Testing mode:  Enable testing mode to test transformations on a sample set of data.

The details about the Transformer constructs are provided in the following document: [**Transformer Constructs**](yaml-transformer-constructs.md)

#### Transformer Modes:

The transformer supports both end-to-end and intermediate mode.
  - End-to-End mode:
        The YAML file has input sources and output configurations along with the transformations that will happen on the inputs.
        Supported input/output formats: hive, orc, parquet, avro, text, csv, json, bigquery, kafka and any custom input supporting Spark SQL input/output specification.
        Following is a sample yaml file which reads and writes to hive:

    ```yaml
    inputs:
        - {type: hive, alias: product, db: dim, table: product}
        - {type: hive, alias: date, db: dim, table: date}
        - {type: hive, alias: transactions, db: stage, table: transactions}
    transformations :
        - description: "SQL can be used for simple transformations"
          input: product
          alias: product_filtered
          persist: memory_only
          sql: "select product_id, product_desc as product from product where product_desc != 'N.A.'"
        - description: "Actions can contain multiple action (transformation) steps"
          input: transactions
          alias: transactions_transformed
          actions:
            - {type: rename, list: {id: trxn_id , loc: location}}
            - {type: transform, list: {location: "lower(location)", trxn_id: "concat(trxn_id, location)", rank: "RANK() OVER (PARTITION BY location order by date desc)"}}
            - {type: join, columns: [product] , join_type: inner, with: product_filtered}
            - {type: join, columns: [date], join_type: inner, with: date}
            - {type: sequence, sk_source: transactions, sk_column: id}
    outputs:
        - {type: hive, input: product_filtered, mode: append, db: stage, table: temp_product}
        - {type: hive, input: transactions_transformed, mode: append, db: stage, table: transformed_transaction}
    ```
    The Yaml file can be executed using the provided utility shell script **sope-spark-submit.sh**. Run 'sope-spark-submit.sh --help' for details on the parameter options.
    Apart from the utility arguments, the standard spark-submit options are to be mentioned with the script itself. These are passed to the spark-submit invocation internally. 
    Note: The spark-submit option for --class and --deploy-mode are not required for the utility script, since they are handled internally.
    Following shows a example usage of the script:
    
    - Client mode:
    ```shell
    ./sope-spark-submit.sh  --yaml_folders "/home/user/yaml-files,/home/user/conf-files" --main_yaml_file demo.yaml --cluster_mode false  --name sope_etl_demo --master yarn  --substitution_files="demo-conf1.yaml, demo-conf2.yaml"
    ```
    
    - Cluster mode:
    ```shell
    ./sope-spark-submit.sh  --yaml_folders "/home/user/yaml-files,/home/user/conf-files" --main_yaml_file demo.yaml --cluster_mode true  --name sope_etl_demo --master yarn  --substitution_files="demo-conf1.yaml, demo-conf2.yaml"
    ```
    
   -   Intermediate mode:
        This mode allows using yaml file in Scala/Java code. Multiple dataframes can be provided as input and the final output will give references to transformed dataframes for all transformation aliases.

        Following is a sample yaml file for intermediate mode use:
        ```yaml
        inputs: [transactions, product, date, dim_product]
        transformations :
            - input: product
              alias: product_filtered
              sql: "select product_id, product_desc as product from product where product_desc != 'N.A.'"
            - input: transactions
              alias: trxn_transformed
              actions:
                - {type: rename, list: {id: trxn_id , loc: location}}
                - {type: transform, list: {location: lower(location), trxn_id: "concat(trxn_id, location)", rank: "RANK() OVER (PARTITION BY location order by date desc)"}}
                - {type: join, with: product_filtered,  columns: [product] , join_type: inner}
                - {type: join, with: date, columns: [date], join_type: inner}
                - {type: select_not, columns: [location]}
            - input: transactions
              alias: grp_by
              actions:
                - {type: group_by, columns: [product], expr: "count(product) as p_cnt"}
                - {type: order_by, columns: [p_cnt:desc]}
            - input : product
              alias: scd_product
              persist: true
              actions:
                - {type: scd, dim_table: dim_product, sk_column: product_key, natural_keys: [product_id], derived_columns: [derived_attr], meta_columns: [last_updated_date]}
            - input: scd_product
              alias: insert_update
              actions:
                - {type: filter, condition: "scd_status = 'INSERT' OR scd_status = 'UPDATE'"}
                - {type: transform, list: {last_updated_date: current_date}}
                - {type: sequence, sk_source: dim_product, sk_column: product_key}
            - input: scd_product
              alias: final_dim_out
              actions:
                - {type: filter, condition: "scd_status = 'NCD'"}
                - {type: union, with: [insert_update]}
        ```
        The above yaml file can be called in code as follows:
        ```scala
        import com.sope.common.yaml.YamlFile.IntermediateYaml
        val transactionsDF = ...
        val productDF = ...
        val dateDF = ...
        // The YAML files should be added in spark's driver classpath, returns list of Tuple of alias and transformed dataframes
        val transformationResult = IntermediateYaml("withoutSourceInfo.yaml").getTransformedDFs(transactionsDF, productDF, dateDF)
        // Get last transformed dataframe
        val transformedDF = transformationResult.last._2
        ```

#### Optimizations:
The Yaml Transformer will try to figure out if there are any transformations that are being reused and persist them using MEMORY_ONLY mode. 
This may be useful if you do not want to explicitly tag the transformation for persistence and let the transformer decide on it.

Also, if the transformation to be persisted is being used for multiple joins, it will be pre-partitioned on the join columns that are involved in most joins.
This feature is enabled by default. To deactivate auto-persist set the 'auto_persist' option to false through the utility shell.

#### Streaming support:
The Transformer also supports Structured streaming mode. The input and output specification for streaming mode can be found in the
transformer constructs page. Following shows a sample use of streaming mode:

```yaml
   inputs:
      - {type: csv, alias: streaming_input, path: "hdfs://streaming-folder", options: {header: true}, is_streaming: true, schema_file: csv_schema.yaml}
   transformations :
      - input: streaming_input
        alias: grouped_event_time
        actions:
         - {type: watermark, event_time: time, delay_threshold: "24 hours"}
         - {type: group_by, columns: ["window(time, '1 minutes')", age], expr: "count(*)"}
   outputs:
      - {type: custom, input: grouped_event_time, is_streaming: true, format: console, output_mode: complete}
```


#### Templates:
The Transformer supports a 'yaml' action construct which can be used to call another yaml. It also supports substitution to drive the templates using dynamic values.
There are some etl specific templates that are provided for reference:
- [SCD Template](templates/scd_template.yaml)
- [DQ Template](templates/data_quality_template.yaml)

#### Custom User Defined Functions:
An interface 'com.sope.etl.register.UDFRegistration' is provided for registering custom UDFs for use in the Transformer.
You can create a jar for custom UDFs and register with Transformer using 'custom_udfs_class' option on the utility shell.
There is also a way to provide Scala UDFs in the YAML file itself. Following sample shows how to provide dynamic UDFs:

```yaml
    udfs: {
      custom_upper: "(desc: String) =>  if(desc.startsWith("P")) desc.toUpperCase else desc"
      }
    inputs:
        - {type: hive, alias: product, db: dim, table: product}
        - {type: hive, alias: date, db: dim, table: date}
        - {type: hive, alias: transactions, db: stage, table: transactions}
    transformations :
        - description: "SQL can be used for simple transformations"
          input: product
          alias: product_filtered
          persist: memory_only
          sql: "select product_id, custom_upper(product_desc) as product from product where product_desc != 'N.A.'"
          ....
```

Example invocation for registering custom udfs jar:

```shell
./sope-spark-submit.sh  --yaml_folders "/home/user/yaml-files" --main_yaml_file demo.yaml --cluster_mode true  --custom_udfs_class=com.custom.CustomUDFs --name sope_etl_demo --master yarn --jars custom_udf.jar
```

#### Custom Transformation:
If there is a need to call some complex/pre-built logic developed in using Scala/Java SparkSQl API's, they can be integrated by calling the 'named_transform' action construct.
For integration, you need to implement the 'com.sope.etl.register.TransformationRegistration' trait and define the 'registerTransformations' which returns a Map of transformation name and transformation function (DataFrame*) => DataFrame.
The jar needs to be added to Spark's classpath and the class is to be registered using the utility shell option 'custom_transformations_class'

Example invocation for registering transformations jar: 
```shell
./sope-spark-submit.sh  --yaml_folders "/home/user/yaml-files" --main_yaml_file demo.yaml --cluster_mode true  --custom_transformations_class=com.custom.CustomTranformations --name sope_etl_demo --master yarn --jars custom_transformations.jar
```

#### Testing mode:
The testing mode allows sampling of data for all sources at once, without the need to edit the Yaml transformation file.
This is helpful if you need to test the transformation pipeline on a small subset of data.
To enable testing mode, set the 'testing_mode' during invocation using the utility shell. You can also control the data fraction using the 'testing_data_fraction' option (Optional, if not provided defaults to 0.1).

Example:
```shell
./sope-spark-submit.sh  --yaml_folders "/home/user/yaml-files" --main_yaml_file demo.yaml --cluster_mode true  --testing_mode=true --testing_data_fraction=0.2 --name sope_etl_demo --master yarn
```