package daslab.exp1;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * @author zyz
 * @version 2018-05-30
 */
@SuppressWarnings("Duplicates")
public class Exp1Cluster {
    private SparkSession sparkSession;

    private final static int DAY_START = 1;
    private final static int DAY_TOTAL = 2;
    private final static String ACCURATE_SAVE_PATH = "/tmp/ada/exp/exp_acc.csv";

    // key: which time(day * 24 + hour); value: 8 queries' results
    private Map<String, List<ResultUnit>> accurateResults;

    private int currentDay;
    private int currentHour;

    private final static List<String> QUERIES = ImmutableList.of(
            "SELECT AVG(page_count) FROM pagecounts",
            "SELECT AVG(page_count) FROM pagecounts WHERE page_size>80000",
            "SELECT AVG(page_count) FROM pagecounts WHERE project_name='aa'",
            "SELECT AVG(page_count) FROM pagecounts WHERE project_name='kk'",
            "SELECT COUNT(page_count) FROM pagecounts",
            "SELECT COUNT(page_count) FROM pagecounts WHERE page_size>80000",
            "SELECT COUNT(page_count) FROM pagecounts WHERE project_name='aa'",
            "SELECT COUNT(page_count) FROM pagecounts WHERE project_name='kk'"
    );

    public Exp1Cluster() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - Exp1 on Lab Cluster")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://ubuntu1:9000/zyz/wiki/")
                .config("spark.executor.memory", "16g")
                .config("spark.dynamicAllocation.enabled", true)
                .config("spark.dynamicAllocation.initialExecutors", 16)
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        accurateResults = Maps.newHashMap();
    }


    private void initialize() {
        execute("DROP DATABASE IF EXISTS wiki_ada CASCADE");
        execute("CREATE DATABASE wiki_ada");
        execute("USE wiki_ada");
        execute("CREATE EXTERNAL TABLE pagecounts(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/zyz/wiki'");
        for (int day = 1; day <= DAY_START; day++) {
            for (int hour = 0; hour < 24; hour++) {
                String path = String.format("/zyz/raw/n_pagecounts-201601%02d-%02d0000", day, hour);
                String command = "hadoop fs -cp " + path + " /zyz/wiki";
                AdaLogger.debug(this, "Loading " + path + " into table");
                systemCall(command);
            }
        }
        currentDay = DAY_START + 1;
        currentHour = 0;
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

    private void appendData() {
        if (currentDay > 21) {
            return;
        }
        String path = String.format("/zyz/raw/n_pagecounts-201601%02d-%02d0000", currentDay, currentHour);
        String command = "hadoop fs -cp " + path + " /zyz/wiki";
        AdaLogger.debug(this, "Loading " + StringUtils.substringAfterLast(path, "/") + " into table");
        systemCall(command);
        currentHour++;
        if (currentHour > 23) {
            currentHour = 0;
            currentDay++;
        }
    }

    private List<ResultUnit> runAccurate(int hours) {
        List<ResultUnit> results = Lists.newArrayList();
        for (String query : QUERIES) {
            AdaLogger.info(this, "About to run(accurate) {" + query + "}");
            Dataset<Row> rows = sparkSession.sql(query);
            if (query.contains("AVG")) {
                results.add(new ResultUnit(hours, rows.collectAsList().get(0).getDouble(0)));
            } else if (query.contains("COUNT")) {
                results.add(new ResultUnit(hours, (double) rows.collectAsList().get(0).getLong(0)));
            }
        }
        return results;
    }

    private void saveAccurate() {
        try {
            FileWriter fileWriter = new FileWriter(new File(ACCURATE_SAVE_PATH));
            StringBuilder header = new StringBuilder("date");
            for (int i = 0; i < QUERIES.size(); i++) {
                header.append(",q").append(i + 1);
            }
            header.append("\r\n");
            fileWriter.write(header.toString());
            accurateResults.forEach((time, results) -> {
                try {
                    StringBuilder body = new StringBuilder(time);
                    for (int i = 0; i < QUERIES.size(); i++) {
                        body.append(",").append(results.get(i).result);
                    }
                    body.append("\r\n");
                    fileWriter.write(body.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        initialize();

        String time;
        for (int i = DAY_START + 1; i <= DAY_TOTAL; i++) {
            for (int j = 0; j < 24; j++) {
                int day = j - 1 < 0 ? i - 1 : i;
                int hour = j - 1 < 0 ? 23 : j - 1;
                time = String.format("%02d%02d", day, hour);
                List<ResultUnit> results = runAccurate(day * 24 + hour + 1);
                AdaLogger.info(this, "Date " + time + "'s results: ");
                for (int k = 0; k < results.size(); k++) {
                    AdaLogger.info(this, "Query No." + k + ": " + results.get(k).toString());
                }
                accurateResults.put(time, results);
                appendData();
            }
        }
        time = String.format("%02d%02d", DAY_TOTAL, 23);
        accurateResults.put(time, runAccurate(DAY_TOTAL * 24 + 24));
        saveAccurate();
    }

    public static void main(String[] args) {
        Exp1Cluster exp1Cluster = new Exp1Cluster();
        exp1Cluster.run();
    }
}
