package daslab.restore;

import daslab.utils.AdaLogger;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

@SuppressWarnings("Duplicates")
public class DatabaseRestore implements RestoreModule {
    private SparkSession sparkSession;

    public DatabaseRestore() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - DatabaseRestore")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                .config("spark.executor.memory", "12g")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
    }

    public void restore() {
        execute("DROP DATABASE IF EXISTS wiki_ada CASCADE");
        execute("CREATE DATABASE wiki_ada");
        execute("USE wiki_ada");
        execute("create table pagecounts(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        execute("create table pagecounts_batch(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        for (int day = 1; day <= 7; day++) {
            for (int hour = 0; hour < 24; hour++) {
                execute(String.format("LOAD DATA LOCAL INPATH '/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000' INTO TABLE pagecounts", day, hour));
            }
        }

        AdaLogger.info(this, "Restored database to initial status.");

        try {
            VerdictSpark2Context verdictSpark2Context = new VerdictSpark2Context(sparkSession.sparkContext());
            verdictSpark2Context.sql("DROP SAMPLES OF wiki_ada.pagecounts");
            verdictSpark2Context.sql("CREATE 10% UNIFORM SAMPLE OF wiki_ada.pagecounts");
        } catch (VerdictException e) {
            e.printStackTrace();
        }

        AdaLogger.info(this, "Restored sample to initial status.");

        sparkSession.close();
    }

    private void execute(String sql) {
        AdaLogger.debug(this, "About to run: " + sql);
        sparkSession.sql(sql);
    }

    private void systemCall(String cmd) {
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(process.getInputStream())));
            String line;
            while ((line = bufferedReader.readLine()) != null)
                AdaLogger.debug(this, "System print: " + line);
            if (process.waitFor() != 0) {
                if (process.exitValue() == 1) {
                    AdaLogger.error(this, "Call {" + cmd + "} error!");
                }
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        DatabaseRestore d = new DatabaseRestore();
        d.restore();
    }
}
