
Sope
====

> Marathi, IPA: /sə/ /o/ /pə/ /e/ (Adjective: Achieved without great effort)

**Sope** is set of utilities and library functions that helps with ETL development using **Apache Spark**.
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
By default the project if built using Spark version 2.4 (scala 2.11), which should be compatible with all 2.x versions.
If you need to override the spark/scala version, use 'custom' profile for build as follows:
**mvn package -Pcustom  -Dspark.version=2.4.0 -Dscala.version.tag=2.12 -Dscala.version=2.12.1**


##### Contributions
Please feel free to add issues to report any bugs/propose addition of new features.