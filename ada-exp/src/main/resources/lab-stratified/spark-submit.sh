#!/usr/bin/env bash

# run exp16 time cost with increasing stratified groups
~/spark-2.2.1-bin-hadoop2.7/bin/spark-submit --master spark://ubuntu1:7077 --class daslab.exp.App --driver-memory 64g ~/zyz/ada-exp-1.0-SNAPSHOT-jar-with-dependencies.jar exp16_stratified_cost
