# Node Measures

Node measures based on Spark GraphX

## included algorithoms

* Degree Centrality
* Eigenvector Centrality
* Closeness Centrality
* Betweenness Centrality
* Local Clustering Coefficient
* Community Detection (Louvain)

## Architecture

Actually I built a parent project which includes several sub projects.
As a result each sub project has no connection to the others

## How to run

Take degree centrality as an example:

1. You need to package the project, run ```mvn clean package``` in the root directory of this project. After that, you
can find two jars in each sub project directory.

2. In the degree centrality package, in target directory, you can find two jars, one includes the dependencies, the other one 
does not include dependencies. Upload two jars to your cluster.

3. Run commands below, you can replace executor parameters on your own and add you own input path, output path and number of partitions.
the parameters of main functions are required differently, check parameters in each main class file.
```bash
spark-submit --class org.ymt.spark.graphx.degree.DegreeCentrality \ 
--master yarn --deploy-mode cluster --executor-memory 30G --executor-cores 15 \ 
--num-executors 15 --driver-memory 40G --driver-cores 30 Degree-1.0.jar inputpath outputpath numberOfPartitions
```

4. You can find the output files in the output path.



