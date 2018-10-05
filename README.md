
Sope
====

> Marathi, IPA: /sə/ /o/ /pə/ /e/ (Adjective: Achieved without great effort)

**sope** is set of utilities and library functions that helps with ETL development using **Apache Spark**.
The project contains following sub-modules
- *sope-spark*
- *sope-etl*

1. **sope-spark**:
 This module contains library functions and a Scala internal **dsl** library that assists with writing **Spark SQL** ETL transformations in concise manner
 
    [More information](sope-spark/README.md)
 
2. **sope-etl**:
 This module contains a **YAML** based external transformer with easy to use ETL constructs.
 
    [More information](sope-etl/README.md)


##### Building the project:
The project only supports Spark versions 2.x onwards.
Use **mvn clean package** to build the project. Import the generated jars in your project.