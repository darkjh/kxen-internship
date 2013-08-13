kxen-internship
===============

My internship projects in KXEN Inc.

This is a brief readme, my internship report contains detailed information.

## Components ##

- Prototypes for bipartite graph projection problem
    + SQL inner join approache with Pig and Hive
    + FP-Tree approach (2 implementations)
    + Parallel FP-Tree approach with Hadoop
    + Apache Giraph based approach
- Apache Mahout test

## Compile ##

```sh
mvn -DskipTests install
mvn package
```

The jar file is in target/ folder of each sub-module.

## Dependencies ##

- Apache Mahout 0.8
- Apache Giraph 1.1.0-SNAPSHOT

Maybe these 2 dependencies are not in the maven central repo, in which case you need to compile and install them first. Edit project's pom.xml file accordingly.
