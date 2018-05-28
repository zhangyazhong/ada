#!/usr/bin/env bash

# run ada-core on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.App ~/ada/ada-core-1.0-SNAPSHOT-jar-with-dependencies.jar


# run spark-shell on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-shell --master spark://master:7077 --conf spark.sql.warehouse.dir=hdfs://master:9000/home/hadoop/spark/


# run restore module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar restore


# run exp1 module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1
