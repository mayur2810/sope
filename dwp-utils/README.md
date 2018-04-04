## dwp-utils

This module contains following frameworks :
 1. *YAML Transformer*: 
 
    A yaml based external transformer with easy to use ETL constructs. It is useful for creating simple and monotonous transformations
    by using a external YAML file, instead of writing scala/java files. The transformer supports both end-to-end and intermediate mode.
    
    -   End-to-End mode:
        The YAML files has input sources and output targets information along with the transformation that will happen on the inputs.
        Supported input/output formats: hive, orc, parquet, text.
        
        Following is a sample yaml file which reads and writes to hive:
        ```yaml
        inputs:
            - {type: hive, alias: product, db: gold, table: product}
            - {type: hive, alias: date, db: gold, table: date}
            - {type: hive, alias: transactions, db: work, table: transactions}
        transformations :
            - input: "product"
              alias: "product_filtered"
              persist: true
              transform:
                - {type: filter, condition: "product_desc != 'N.A.'"}
                - {type: rename, list: {product_desc: "product"}}
                - {type: select, columns: ["product_id", "product"]}
            - input: "transactions"
              transform:
                - {type: rename, list: {id: "trxn_id" , loc: "location"}}
                - {type: transform, list: {location: "lower(location)", trxn_id: "concat(trxn_id, location)", rank: "RANK() OVER (PARTITION BY location order by date desc)"}}
                - {type: join, columns: ["product"] , join_type: inner, with: "product_filtered"}
                - {type: join, columns: ["date"], join_type: inner, with: "date"}
                - {type: sequence, sk_source: "transactions", sk_column: "id"}
        outputs:
            - {type: hive, input: "product_filtered", mode: append, db: work, table: "temp_product"}
            - {type: hive, input: "transactions", mode: append, db: work, table: "transformed_transaction"}

        ```
 
    -   Intermediate mode:
        This mode allows to use yaml file in Scala/Java code. Multiple dataframes can be provided as input and the final output will
        give references to transformed dataframes at all stages.
        
        Following is a sample yaml file for intermediate mode use:
        ```yaml
        inputs: ["transactions", "product", "date"]
        transformations :
            - alias: "product_filtered"
              persist: true
              input: "product"
              transform:
                - {type: filter, condition: "product_desc != 'N.A.'"}
                - {type: rename, list: {product_desc: "product"}}
                - {type: select, columns: ["product_id", "product"]}
            - input: "transactions"
              transform:
                - {type: rename, list: {id: "trxn_id" , loc: "location"}}
                - {type: transform, list: {location: "lower(location)", trxn_id: "concat(trxn_id, location)", rank: "RANK() OVER (PARTITION BY location order by date desc)"}}
                - {type: join, columns: ["product"] , join_type: inner, with: "product_filtered"}
                - {type: join, columns: ["date"], join_type: inner, with: "date"}
                - {type: sequence, sk_source: "transactions", sk_column: "id"}
        ```
        The above yaml file can be called in code as follows:
        ```scala
        import com.mayurb.dwp.transform.YamlDataTransform
        val transactionsDF = ...
        val productDF = ...
        val dateDF = ...
        val ydt = new YamlDataTransform("withoutSourceInfo.yaml", transactionsDF, productDF, dateDF)
        // returns list of Tuple of alias and transformed dataframe
        val transformationResult = ydt.getTransformedDFs
        // Get last transformed dataframe
        val transfomedDF = transformationResult.last._2
        ```
 
 2. *DQ Framework*: 
 
    A simplified and extensible yaml based Data Quality and Data Standardization framework
    *TBD*