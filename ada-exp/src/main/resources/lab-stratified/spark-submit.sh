#!/usr/bin/env bash

# run spark-shell on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-shell --master spark://ubuntu1:7077 --driver-memory 64g --executor-memory 64g --conf spark.sql.warehouse.dir=hdfs://ubuntu1:9000/zyz/spark

# run exp16 time cost with increasing stratified groups
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp16_stratified_cost
