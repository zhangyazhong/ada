package daslab.exp;

import daslab.utils.AdaLogger;
import daslab.utils.AdaSystem;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public abstract class ExpTemplate {
    private static SparkSession sparkSession;
    private static VerdictSpark2Context verdictSpark2Context;

    public ExpTemplate(String name) {
        if (sparkSession == null) {
            sparkSession = SparkSession
                    .builder()
                    .appName(name)
                    .enableHiveSupport()
                    .config("spark.sql.warehouse.dir", ExpConfig.get("spark.sql.warehouse.dir"))
                    .config("spark.executor.memory", ExpConfig.SPARK_EXECUTOR_MEMORY)
                    .config("spark.driver.memory", ExpConfig.SPARK_DRIVER_MEMORY)
                    .getOrCreate();
            sparkSession.sparkContext().setLogLevel("ERROR");
        } else {
            sparkSession.conf().set("spark.app.name", name);
        }
        try {
            if (verdictSpark2Context == null) {
                verdictSpark2Context = new VerdictSpark2Context(sparkSession.sparkContext());
                verdictSpark2Context.sql("USE " + ExpConfig.get("table.schema"));
            }
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    public ExpTemplate execute(String sql) {
        AdaLogger.debug(this, "About to run: " + sql);
        sparkSession.sql(sql);
        return this;
    }

    public SparkSession getSpark() {
        return sparkSession;
    }

    public VerdictSpark2Context getVerdict() {
        return verdictSpark2Context;
    }

    public abstract void run();

    public void append(String schema, String table, String path) {
        String command = "hadoop fs -cp " + path + " " + ExpConfig.get("table.location");
        AdaLogger.debug(this, "Loading " + path + " into table");
        AdaSystem.call(command);
    }

    public void append(String schema, String table, int day, int hour) {
        String path = String.format("/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000", day, hour);
        append(schema, table, path);
    }

    public void append(int day, int hour) {
        append(ExpConfig.get("table.schema"), ExpConfig.get("table.name"), day, hour);
    }

    public void save(ExpResult result, String path) {
        File file = new File(path);
        file.getParentFile().mkdirs();
        try {
            FileWriter fileWriter = new FileWriter(file);
            String header = StringUtils.join(result.getHeader().toArray(), ",");
            fileWriter.write(header + "\r\n");
            final StringBuilder content = new StringBuilder();
            result.getRowKeys().forEach(key -> content.append(key).append(",").append(StringUtils.join(result.getColumns(key).toArray(), ",")).append("\r\n"));
            fileWriter.write(content.toString());
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
