#!/usr/bin/env bash

# run spark-shell on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-shell --master spark://ubuntu1:7077 --conf spark.sql.warehouse.dir=hdfs://ubuntu1:9000/zyz/spark


# run restore module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar restore
# run clean verdict sample module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar clean_sample


# run exp1 module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1
# run exp1 module on lab cluster
~/spark-2.0.0/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1_cluster
# run exp1 module only accurate
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1_accurate
# run exp1 module collect results
java -jar ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp1_collect


# run exp2 core module on spark cluster
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp2_core
# run exp2 sender module on spark cluster
java -jar ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp2_sender


# run exp3
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3
# run exp3 with database restore
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_with_restore
# clean exp3 database
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_clean
# run exp3 only uniform
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_uniform
# run exp3 only stratified
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_stratified
# run exp3 different ratio
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp3_ratio


# run exp4 accurate result
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_accurate
# run exp4 verdict result
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_verdict
# run exp4 ada result
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_ada
# run exp4 comparison module to compare results between ada and verdict
java -jar ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_comparison
# run exp4 ada time cost
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_verdict_cost
# run exp4 ada time cost
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp4_ada_cost


# run exp5 ada time cost
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp5_ada_cost
# run exp5 verdict time cost
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp5_verdict_cost
# run exp5 ada result performance
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp5_ada_result
# run exp5 verdict result performance
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp5_verdict_result
# run exp5 accurate result performance
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp5_accurate_result
# run exp5 result comparison
java -jar ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp5_compare_result


# run exp6 database variance
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp6_variance


# run exp7 ada time cost (tpc-h)
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp7_ada_cost
# run exp7 accurate result performance (tpc-h)
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp7_accurate_result




# run exp8 big data sampling
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp8_sampling
# run exp8 copy big data
java -jar ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp8_copy
# run exp8 zip tpc-h data
java -jar ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp8_zip
# run exp8 create database for TPC-H
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp8_create



# run exp11 ada with adaptive time cost
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp11_ada_cost
# run exp11 ada with adaptive result performance
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp11_adaptive_result
