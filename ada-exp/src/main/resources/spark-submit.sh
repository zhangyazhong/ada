#!/usr/bin/env bash

# run ada-core on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.App ~/ada/ada-core-1.0-SNAPSHOT-jar-with-dependencies.jar


# run spark-shell on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-shell --master spark://master:7077 --conf spark.sql.warehouse.dir=hdfs://master:9000/home/hadoop/spark/


# run restore module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar restore
# run clean verdict sample module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar clean_sample


# run exp1 module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1
# run exp1 module on lab cluster
~/spark-2.0.0/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1_cluster
# run exp1 module only accurate
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1_accurate
# run exp1 module collect results
java -jar ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1_collect


# run exp2 core module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp2_core
# run exp2 sender module on spark cluster
java -jar ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp2_sender


# run exp3
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3
# run exp3 with database restore
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_with_restore
# clean exp3 database
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_clean
# run exp3 only uniform
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_uniform
# run exp3 only stratified
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_stratified
# run exp3 different ratio
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_ratio



# run exp4 accurate result
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_accurate
# run exp4 verdict result
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_verdict
# run exp4 ada result
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://master:7077 --class daslab.exp.App ~/ada/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_ada