##spark-utils
This module contains library functions and an internal dsl library that helps with writing **Spark SQL** ETL transformations in concise manner.
It will reduce the boiler-plate code for complex transformations using core Spark-SQL API and will assist in code readability and review.

1. Using Library functions:
   
   Just use following import statement and you are all set. 
   ```scala
    import com.mayurb.spark.sql._
   ```
   
   This will make available useful implicit methods on the DataFrame object. 
   Following is the list of some methods available:
 
      ```scala
    df // a dataframe object reference 
    
    // Check if Dataframe is not empty
    df.isNotEmpty  
    
    // Check if Dataframe is empty
    df.isEmpty    
    
    // Rename columns
    df.renameColumns(Map("nme" -> "name", "address" -> "address_line_1"))
    
    // Apply string expressions to provided columns
    df.applyStringExpressions(Map("name" -> "upper(name)", "valid_name" -> "'YES'"))    
     
    // Apply column expressions to provided columns
    df.applyColumnExpressions(Map("name" -> upper($"name"), "valid_name" -> lit("YES"))     
    
    // Cast columns, converts all columns from one type to other castable data type
    df.castColumns(TimestampType, DateType)  /* converts all timestamp columns to date columns */
 
    // Partition Dataframe
    val (indiaDF, otherDF) = df.partition("country = 'India'")
 
    // Get max value for a surrogate key column, empty set returns 0.
    val maxMemberId = df.maxKeyValue("member_id")
 
    // Generate Sequence Number from the provided start index
    df.generateSequence(maxMemberId) // If already has first column as member_id
    df.generateSequence(maxMemberId, Some("member_id")) // will create new column 'member_id' as sequence column
 
    // Flattens the columns in a struct type column to normal columns
    df.unstruct("column")
 
    //Update/Add Surrogate key columns by joining on the 'Key' dimension table.
    val dimDateDF = sqlContext.table("dim_date")
    val dateColumns = Seq("sale_date", "create_date")
    df.updateKeys(dateColumns, dimDateDF, "date" /*join column*/, "date_key" /*key column*/) // new columns - sale_date_key, create_date_key
     
      ```
      
2. Scala DSL:
    A simple DSL for Transformations like : **t1 + t2 + t3 .... + tn --> df** is provided. 
    The DSL can be used to reduce the intermediate dataframe object references and create transformation references instead.
    These transformation references are reusable and concise in representation. You can also use the implicit functions and DSL together.
    
    Following is a sample DSL usage :
    ```scala
    // Use Following import to use the DSL constructs
    import com.mayurb.spark.sql.dsl._
 
    val studentDF =  ...
    val classDF = ...
    // Rename column transformation
    val rename = Rename("first_name" -> "firstname", "last_name" -> "lastname")
    // Column transform transformation
    val transform = Transform("firstname" -> upper($"firstname"), "lastname" -> upper($"lastname"))
    // Join transformation
    val join = Join("cls") << classDF // left join on class DF
    // chain all transformations and finally apply on dataframe
    val transformed = rename + transform + join + Sequence(0l, "id") --> studentDF
    ```
    
    Following DSL constructs are supported. Please refer scala doc for more details:
    - Select
    - SelectNot
    - Filter
    - Rename
    - Drop
    - Transform
    - Sequence
    - Join
    - Cube
    - Group
    - Unstruct