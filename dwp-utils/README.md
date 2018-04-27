## dwp-utils

This module contains following framestages :
 1. *YAML Transformer*: 
 
    A yaml based external transformer with easy to use ETL constructs. It is useful for creating simple and monotonous transformations
    by using a external YAML file, instead of writing scala/java files. The transformer supports both end-to-end and intermediate mode.
    
    -   End-to-End mode:
        The YAML files has input sources and output targets information along with the transformation that will happen on the inputs.
        Supported input/output formats: hive, orc, parquet, text.
        
        Following is a sample yaml file which reads and writes to hive:
        ```yaml
        inputs:
            - {type: hive, alias: product, db: dim, table: product}
            - {type: hive, alias: date, db: dim, table: date}
            - {type: hive, alias: transactions, db: stage, table: transactions}
        transformations :
            - input: "product"
              alias: "product_filtered"
              persist: true
              actions:
                - {type: filter, condition: "product_desc != 'N.A.'"}
                - {type: rename, list: {product_desc: "product"}}
                - {type: select, columns: ["product_id", "product"]}
            - input: "transactions"
              actions:
                - {type: rename, list: {id: "trxn_id" , loc: "location"}}
                - {type: transform, list: {location: "lower(location)", trxn_id: "concat(trxn_id, location)", rank: "RANK() OVER (PARTITION BY location order by date desc)"}}
                - {type: join, columns: ["product"] , join_type: inner, with: "product_filtered"}
                - {type: join, columns: ["date"], join_type: inner, with: "date"}
                - {type: sequence, sk_source: "transactions", sk_column: "id"}
        outputs:
            - {type: hive, input: "product_filtered", mode: append, db: stage, table: "temp_product"}
            - {type: hive, input: "transactions", mode: append, db: stage, table: "transformed_transaction"}

        ```
        
        The yaml file can be submitted for execution using spark-submit command as follows:
        ```shell
           spark-submit  --master yarn  --deploy-mode client --class  com.mayurb.dwp.transform.YamlRunner dwp-utils-x.x.jar --yaml_file test.yaml
        ```
 
    -   Intermediate mode:
        This mode allows to use yaml file in Scala/Java code. Multiple dataframes can be provided as input and the final output will
        give references to transformed dataframes at all stages.
        
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
        import com.mayurb.dwp.transform.YamlDataTransform
        val transactionsDF = ...
        val productDF = ...
        val dateDF = ...
        val ydt = new YamlDataTransform("withoutSourceInfo.yaml", transactionsDF, productDF, dateDF, productDimDF)
        // returns list of Tuple of alias and transformed dataframe
        val transformationResult = ydt.getTransformedDFs
        // Get last transformed dataframe
        val transformedDF = transformationResult.last._2
        ```
        
        Refer this documentation for YAML Transformer Constructs: [YAML Constructs](yaml-transformer-constructs.md)
 
 2. *DQ Framework*:
 
    A simplified and extensible yaml based Data Quality and Data Standardization framework