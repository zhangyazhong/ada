package daslab.exp;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class ExpConfig {
    public final static int DAY_START = 1;
    public final static int DAY_TOTAL = 2;
    public final static int[] UNIFORM_SAMPLE_RATIO = {10};
    public final static int[] STRATIFIED_SAMPLE_RATIO = {10};
    public final static String[] STRATIFIED_SAMPLE_COLUMN = {"project_name"};

    public final static int HOUR_START = 24 * 1 - 1;
    public final static int HOUR_TOTAL = 24 * 2;

    public final static String SPARK_EXECUTOR_MEMORY = "16g";
    public final static String SPARK_DRIVER_MEMORY = "4g";

    public final static String WAREHOUSE = "hdfs://master:9000/home/hadoop/spark/";
    public final static Map<String, Object> ENV = new ImmutableMap.Builder<String, Object>()
            .put("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
            .put("table.location", "/home/hadoop/spark/wiki_ada_pagecounts")
            .put("table.schema", "wiki_ada")
            .put("table.name", "pagecounts")
            .build();

    public static String get(String key) {
        return ENV.get(key).toString();
    }

    public static String getString(String key) {
        return ENV.get(key).toString();
    }
}
