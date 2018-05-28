package daslab.exp1;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.utils.AdaLogger;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author zyz
 * @version 2018-05-28
 */
public class Exp1 {
    private SparkSession sparkSession;
    private VerdictSpark2Context verdictSpark2Context;
    // key: which sample; value: 8 queries' results
    private Map<Integer, List<ResultUnit>> approximateResults;
    // key: which time(day * 24 + hour); value: 8 queries' results
    private Map<String, List<ResultUnit>> accurateResults;
    // key: which time(day * 24 + hour); value: 8 queries' hit count
    private Map<String, List<Integer>> performances;
    private int currentDay;
    private int currentHour;

    private final static int SAMPLE_COUNT = 100;
    private final static String SAMPLE_RATIO = "10%";
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
    private final static int DAY_START = 7;
    private final static int DAY_TOTAL = 21;
    private final static String SAVE_PATH = "/tmp/ada/exp/exp1.csv";

    public Exp1() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - Exp1")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        try {
            verdictSpark2Context = new VerdictSpark2Context(sparkSession.sparkContext());
            verdictSpark2Context.sql("USE wiki_ada");
        } catch (VerdictException e) {
            e.printStackTrace();
        }
        approximateResults = Maps.newHashMap();
        accurateResults = Maps.newHashMap();
        performances = Maps.newLinkedHashMap();
    }

    private void initialize() {
        execute("DROP DATABASE wiki_ada CASCADE");
        execute("CREATE DATABASE wiki_ada");
        execute("USE wiki_ada");
        execute("create table pagecounts(date_time int, project_name string, page_name string, page_count int, page_size int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        for (int day = 1; day <= DAY_START; day++) {
            for (int hour = 0; hour < 24; hour++) {
                String path = String.format("'/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000'", day, hour);
                execute("LOAD DATA INPATH " + path + " INTO TABLE pagecounts");
            }
        }
        currentDay = 8;
        currentHour = 0;
    }

    private void appendData() {
        if (currentDay > 21) {
            return;
        }
        String path = String.format("'/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000'", currentDay, currentHour);
        execute("LOAD DATA INPATH " + path + " INTO TABLE pagecounts");
        currentHour++;
        if (currentHour > 23) {
            currentHour = 0;
            currentDay++;
        }
    }

    private void execute(String sql) {
        AdaLogger.debug(this, "About to run: " + sql);
        sparkSession.sql(sql);
    }

    private List<ResultUnit> runApproximate() {
        try {
            verdictSpark2Context.sql("DELETE SAMPLES OF pagecounts");
            verdictSpark2Context.sql("CREATE " + SAMPLE_RATIO + " SAMPLE OF pagecounts");
            List<ResultUnit> results = Lists.newArrayList();
            for (String query : QUERIES) {
                AdaLogger.info(this, "About to test(approximate) {" + query + "}");
                Dataset<Row> rows = verdictSpark2Context.sql(query);
                double approximateResult = 0;
                double errorBound = 0;
                if (query.contains("AVG")) {
                    approximateResult = rows.collectAsList().get(0).getDouble(0);
                    errorBound = rows.collectAsList().get(0).getDouble(1);
                } else if (query.contains("COUNT")) {
                    approximateResult = (double) rows.collectAsList().get(0).getLong(0);
                    errorBound = rows.collectAsList().get(0).getDouble(1);
                }
                results.add(new ResultUnit(approximateResult, errorBound));
            }
            return results;
        } catch (VerdictException e) {
            e.printStackTrace();
        }
        return null;
    }

    private List<ResultUnit> runAccurate() {
        List<ResultUnit> results = Lists.newArrayList();
        for (String query : QUERIES) {
            AdaLogger.info(this, "About to test(accurate) {" + query + "}");
            Dataset<Row> rows = sparkSession.sql(query);
            if (query.contains("AVG")) {
                results.add(new ResultUnit(rows.collectAsList().get(0).getDouble(0)));
            } else if (query.contains("COUNT")) {
                results.add(new ResultUnit((double) rows.collectAsList().get(0).getLong(0)));
            }
        }
        return results;
    }

    private List<Integer> runEvaluate(String time) {
        List<Integer> performance = Lists.newArrayList();
        int hit;
        List<ResultUnit> accurateResults = this.accurateResults.get(time);
        for (int q = 0; q < QUERIES.size(); q++) {
            hit = 0;
            for (int k = 0; k < SAMPLE_COUNT; k++) {
                List<ResultUnit> approximateResults = this.approximateResults.get(k);
                if (Math.abs(accurateResults.get(q).result - approximateResults.get(q).result)
                        <= approximateResults.get(q).errorBound) {
                    hit++;
                }
            }
            performance.add(hit);
        }
        return performance;
    }

    private void save() {
        try {
            FileWriter fileWriter = new FileWriter(new File(SAVE_PATH));
            StringBuilder header = new StringBuilder("date");
            for (int i = 0; i < QUERIES.size(); i++) {
                header.append(",q").append(i + 1);
            }
            header.append("\r\n");
            fileWriter.write(header.toString());
            performances.forEach((time, hits) -> {
                try {
                    StringBuilder body = new StringBuilder(time);
                    for (int i = 0; i < QUERIES.size(); i++) {
                        body.append(hits.get(i));
                    }
                    body.append("\r\n");
                    fileWriter.write(body.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void run() {
        initialize();

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            AdaLogger.info(this, "About to build no." + i + " sample");
            this.approximateResults.put(i, runApproximate());
        }

        String time;
        for (int i = DAY_START + 1; i <= DAY_TOTAL; i++) {
            for (int j = 0; j < 24; j++) {
                int day = j - 1 < 0 ? i - 1 : i;
                int hour = j - 1 < 0 ? 23 : j -1;
                time = String.format("%02d%02d", day, hour);
                accurateResults.put(time, runAccurate());
                appendData();
            }
        }
        time = String.format("%02d%02d", DAY_TOTAL, 23);
        accurateResults.put(time, runAccurate());

        for (int i = DAY_START + 1; i <= DAY_TOTAL; i++) {
            for (int j = 0; j < 24; j++) {
                int day = j - 1 < 0 ? i - 1 : i;
                int hour = j - 1 < 0 ? 23 : j -1;
                time = String.format("%02d%02d", day, hour);
                performances.put(time, runEvaluate(time));
            }
        }
        time = String.format("%02d%02d", DAY_TOTAL, 23);
        performances.put(time, runEvaluate(time));

        save();
    }

    public static void main(String[] args) {
        Exp1 exp1 = new Exp1();
        exp1.run();
    }
}
