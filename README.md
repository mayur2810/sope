# dwp-spark: Data Warehouse Processing utilities for Spark.

**dwp-spark** is set of utilities and library functions that helps with ETL development using **Apache Spark**.
The project contains following sub-modules
- *spark-utils*
- *dwp-utils*

1. **spark-utils**:
 This module contains library functions and an internal dsl library that assists with writing **Spark SQL** ETL transformations in concise manner
 
    [More information](spark-utils/README.md)
 
2. **dwp-utils**:
 This module contains following utilities :
 - *YAML Transformer*: A yaml based external transformer with easy to use ETL constructs.
 - *DQ Framework*: A simplified and extensible yaml based Data Quality and Data Standardization framework
 
    [More information](dwp-utils/README.md)


##### Building the project:
 Currently the project supports Spark version 2.x onwards. Support for Spark version 1.x will be added later.
 Use **mvn clean package** to build the project. Import the generated jars in your project.