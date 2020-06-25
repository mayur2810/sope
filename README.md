
Sope
====

> Marathi, IPA: /sə/ /o/ /pə/ /e/ (Adjective: Achieved without great effort)

[![Build Status](https://travis-ci.org/mayur2810/sope.svg?branch=master)](https://travis-ci.org/mayur2810/sope)


**Sope** is set of utilities and library functions that help with ETL development using **Apache Spark**. At a high level,
the library provides abstraction on top of Spark SQL APIs that makes it easier to develop applications which are based on SQL operations.
The project contains the following sub-modules:

1. **sope-spark**:
 This module contains useful Dataframe functions and a Scala internal **dsl** library that assists with writing **Spark SQL** transformations in a concise manner.

    [More information](sope-spark/README.md)

2. **sope-etl**:
 This module provides a **YAML** based external transformer with easy to use ETL constructs which acts like a configuration/script driven interface.

    [More information](sope-etl/README.md)


#### Examples:
Please check: https://github.com/mayur2810/sope-spark-examples

#### Building the project:
The project has full support for Spark versions 2.x onwards. Please refer the separate branch for 1.x support, which is limited in features.  
Use **mvn clean package** to build the project. The **sope-etl** module will create a zip package containing the utility shell wrapper along with the jar which is to be used for Spark job submission. The jar generated from **sope-spark** module can be imported and used as a library.  
By default, the project is built using latest Spark version 2.4 (scala 2.11), which should be compatible with all 2.x versions.
If you need to override the spark/scala version, you can override the version properties for the build as follows:  
**mvn package -Dspark.version=2.4.0 -Dscala.version.tag=2.12 -Dscala.version=2.12.1**


#### Contributions
Please feel free to add issues to report any bugs or to propose new features.