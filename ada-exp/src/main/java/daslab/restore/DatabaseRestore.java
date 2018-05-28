package daslab.restore;

import daslab.utils.AdaLogger;
import org.apache.spark.sql.SparkSession;

public class DatabaseRestore implements RestoreModule {
    private SparkSession sparkSession;

    public DatabaseRestore() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - DatabaseRestore")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
    }

    public void restore() {
        execute("DROP DATABASE wiki_ada CASCADE");
        execute("CREATE DATABASE wiki_ada");
        execute("USE wiki_ada");
        execute("create table pagecounts(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        execute("create table pagecounts_batch(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        execute("LOAD DATA LOCAL INPATH '/home/hadoop/wiki/n_pagecounts-20160101-000000' INTO TABLE pagecounts");

        AdaLogger.info(this, "Restored database to initial status.");
    }

    private void execute(String sql) {
        AdaLogger.debug(this, "About to run: " + sql);
        sparkSession.sql(sql);
    }

    public static void main(String[] args) {
        DatabaseRestore d = new DatabaseRestore();
        d.restore();
    }
}
