sope-etl
========

#### YAML Transformer: 

The YAML Transformer reads a yaml file and executes the transformations defined in the file. You will find the transformer useful for creating simple or medium complexity transformation pipelines by using a external YAML file, instead of writing scala/java files. 
The transformer supports following useful features:
- Simple: Simple and easy to use constructs with support for SQL.
- Optimizations: Auto persists and pre-sorts persisted data.
- Template support: Create reusable yaml templates (Call yaml within yaml with support for substitutions).
- Custom Transformations/UDF support: Call custom transformations/udfs written in Scala/Java. 
- Testing mode:  Enable testing mode to test transformations on sample set of data. 

The details about the Transformer constructs are provided in following document: [**Transformer Constructs**](yaml-transformer-constructs.md)
	
##### Transformer Modes:

The transformer supports both end-to-end and intermediate mode.
  - End-to-End mode:
        The YAML files has input sources and output targets information along with the transformation that will happen on the inputs.
        Supported input/output formats: hive, orc, parquet, text, csv, json
        Following is a sample yaml file which reads and writes to hive:
        
	```yaml
               inputs:
                   - {type: hive, alias: product, db: dim, table: product}
                   - {type: hive, alias: date, db: dim, table: date}
                   - {type: hive, alias: transactions, db: stage, table: transactions}
               transformations :
                   - alias: "product_filtered"
                      input: "product"
                     persist: "memory_only"
                     actions:
                       - {type: filter, condition: "product_desc != 'N.A.'"}
                       - {type: rename, list: {product_desc: "product"}}
                       - {type: select, columns: ["product_id", "product"]}
                   - alias: "transactions_transformed"
                      input: "transactions"
                     actions:
                       - {type: rename, list: {id: "trxn_id" , loc: "location"}}
                       - {type: transform, list: {location: "lower(location)", trxn_id: "concat(trxn_id, location)", rank: "RANK() OVER (PARTITION BY location order by date desc)"}}
                       - {type: join, columns: ["product"] , join_type: inner, with: "product_filtered"}
                       - {type: join, columns: ["date"], join_type: inner, with: "date"}
                       - {type: sequence, sk_source: "transactions", sk_column: "id"}
               outputs:
                   - {type: hive, input: "product_filtered", mode: append, db: stage, table: "temp_product"}
                   - {type: hive, input: "transactions_transformed", mode: append, db: stage, table: "transformed_transaction"}
	```
        
	The yaml file can be submitted for execution using spark-submit command as follows:
        
	- Cluster mode:
     
     ```shell
           spark-submit  --master yarn  --deploy-mode cluster  --class  com.sope.etl.YamlRunner  --files="entry.yaml, scd.yaml" sope-etl-x.x.jar --main_yaml_file entry.yaml
     ```  
               
	- Client mode with substitutions:

     ```shell
            spark-submit  --master yarn  --deploy-mode client  --class  com.sope.etl.YamlRunner  --driver-class-path="/yaml-folder/" sope-etl-x.x.jar --main_yaml_file entry.yaml --substitutions "[sub1, {k1: v1, k2: v2}]"
     ```
        
*NOTE*: It is mandatory that all the yaml files are in the spark driver classpath. The main_yaml_file argument only specifies the enrty point yaml file.
 
   -   Intermediate mode:
        This mode allows to use yaml file in Scala/Java code. Multiple dataframes can be provided as input and the final output will
        give references to transformed dataframes for all transformation aliases.
        
        Following is a sample yaml file for intermediate mode use:
        ```yaml
        inputs: ["transactions", "product", "date", "dim_product"]
        transformations :
            - input: "product"
              alias: "product_filtered"
              sql: "select product_id, product_desc as product from product where product_desc != 'N.A.'"
            - input: "transactions"
              alias: "trxn_transformed"
              actions:
                - {type: rename, list: {id: "trxn_id" , loc: "location"}}
                - {type: transform, list: {location: "lower(location)", trxn_id: "concat(trxn_id, location)", rank: "RANK() OVER (PARTITION BY location order by date desc)"}}
                - {type: join, with: "product_filtered",  columns: ["product"] , join_type: inner}
                - {type: join, with: "date", columns: ["date"], join_type: inner}
                - {type: select_not, columns: ["location"]}
            - input: "transactions"
              alias: "grp_by"
              actions:
                - {type: "group_by", columns: ["product"], expr: "count(product) as p_cnt"}
                - {type: "order_by", columns: ["p_cnt:desc"]}
            - input : "product"
              alias: "scd_product"
              persist: true
              actions:
                - {type: "scd", dim_table: "dim_product", sk_column: "product_key", natural_keys: ["product_id"], derived_columns: ["derived_attr"], meta_columns: ["last_updated_date"]}
            - input: "scd_product"
              alias: "insert_update"
              actions:
                - {type: filter, condition: "scd_status = 'INSERT' OR scd_status = 'UPDATE'"}
                - {type: transform, list: {last_updated_date: "current_date"}}
                - {type: sequence, sk_source: "dim_product", sk_column: "product_key"}
            - input: "scd_product"
              alias: "final_dim_out"
              actions:
                - {type: filter, condition: "scd_status = 'NCD'"}
                - {type: union, with: ["insert_update"]}
        ```
        The above yaml file can be called in code as follows:
        ```scala
        import com.sope.etl.yaml.YamlFile.IntermediateYaml
        val transactionsDF = ...
        val productDF = ...
        val dateDF = ...
		// The YAML files should be added in spark's driver classpath, returns list of Tuple of alias and transformed dataframes
        val transformationResult = IntermediateYaml("withoutSourceInfo.yaml").getTransformedDFs(transactionsDF, productDF, dateDF)
        // Get last transformed dataframe
        val transformedDF = transformationResult.last._2
        ```
        
##### Optimizations:
The Yaml Transformer will try to figure out if there are any transformations that are being reused and persist them using MEMORY_ONLY mode. This may be useful if you do not want to explicitly tag the transformation for persistence and let the transformer decide on it.

Also, if the transformation to be persisted is being used is referred in multiple joins, the data to be persisted will be pre-sorted on the join columns involved in most joins.
This feature is enabled by default. To deactivate auto-persist set *sope.auto.persist.enabled=false* using --driver-java-options. 
	
##### Templates:
The Transformer supports a 'yaml' action construct which can be used to call another yaml. It also supports substitution mode to drive the templates using dynamic values.
There are some etl specific templates that are provided for reference:
- [SCD Template](src/main/resources/templates/scd_template.yaml)
- [DQ Template](src/main/resources/templates/data_quality_template.yaml)	  
     
##### Custom Transformation:
If there is a need to call some complex/pre-built logic developed in using Scala/Java SparkSQl API's, they can be integrated by calling the 'named_transform' action construct. For integration, you need to implement the 'com.sope.etl.register.TransformationRegistration' trait and define the 'registerTransformations' which return a Map of transformation name and transformation function (DataFrame*) => DataFrame.

An interface 'com.sope.etl.register.UDFRegistration' is also provided for registering custom UDF's for use in transformer.

You can create a jar for custom transformations/udfs and register with Transformer using system properties:
- sope.etl.transformation.class
- sope.etl.udf.class

example: 

```shell
           spark-submit  --master yarn  --deploy-mode cluster  --class  com.sope.etl.YamlRunner  --driver-java-options "-Dsope.etl.transformation.class=com.custom.CustomTranformations -Dsope.etl.udf.class=com.custom.CustomUDFs"  --files="entry.yaml, scd.yaml" --jars "custom.jar" sope-etl-x.x.jar --main_yaml_file entry.yaml
```  

		
##### Testing mode:
The testing mode allows to sample data for all sources at once, without the need to edit the Yaml transformation file.
This is helpful if you need to test transformation pipeline on small subset of data.
To enable testing mode, set following system properties using *driver-java-options* property.
1. sope.testing.mode.enabled=true
2. sope.testing.data.fraction=0.5 (Optional, if not provided defaults to 0.1)
	    