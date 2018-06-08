package daslab.exp1;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import daslab.utils.AdaLogger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author zyz
 * @version 2018-05-30
 */
@SuppressWarnings("Duplicates")
public class Exp1Accurate {
    private SparkSession sparkSession;

    private final static int DAY_TOTAL = 21;
    private final static String ACCURATE_SAVE_PATH = "/tmp/ada/exp/exp_acc2.csv";

    // key: which time(day * 24 + hour); value: 8 queries' results
    private Map<String, List<ResultUnit>> accurateResults;

    /*
    private final static List<String> QUERIES = ImmutableList.of(
            "SELECT SUM(page_count) FROM view_name",
            "SELECT SUM(page_count) FROM view_name WHERE page_size>80000",
            "SELECT SUM(page_count) FROM view_name WHERE project_name='aa'",
            "SELECT SUM(page_count) FROM view_name WHERE project_name='kk'",
            "SELECT COUNT(page_count) FROM view_name",
            "SELECT COUNT(page_count) FROM view_name WHERE page_size>80000",
            "SELECT COUNT(page_count) FROM view_name WHERE project_name='aa'",
            "SELECT COUNT(page_count) FROM view_name WHERE project_name='kk'"
    );
    */
    private final static List<String> QUERIES = ImmutableList.of(
            "SELECT SUM(page_count) FROM view_name",
            "SELECT SUM(page_count) FROM view_name WHERE page_size>80000",
            "SELECT SUM(page_count) FROM view_name WHERE project_name='aa'",
            "SELECT SUM(page_count) FROM view_name WHERE project_name='kk'"
//            "SELECT VAR_POP(page_count) FROM view_name",
//            "SELECT VAR_POP(page_count) FROM view_name WHERE page_size>80000",
//            "SELECT VAR_POP(page_count) FROM view_name WHERE project_name='aa'",
//            "SELECT VAR_POP(page_count) FROM view_name WHERE project_name='kk'",
//            "SELECT COUNT(page_count) FROM view_name",
//            "SELECT COUNT(page_count) FROM view_name WHERE page_size>80000",
//            "SELECT COUNT(page_count) FROM view_name WHERE project_name='aa'",
//            "SELECT COUNT(page_count) FROM view_name WHERE project_name='kk'"
    );

    public Exp1Accurate() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - Exp1 Fast")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                .config("spark.executor.memory", "12g")
//                .config("spark.shuffle.service.enabled", true)
//                .config("spark.dynamicAllocation.enabled", true)
                .getOrCreate();
//        sparkSession = S  parkSession.builder().master("local[*]").appName("Ada Exp - Exp1").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        accurateResults = Maps.newLinkedHashMap();
    }

    public Dataset<Row> readBatch(int day, int hour) {
        String path = String.format("hdfs://master:9000/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000", day, hour);
        StructType schema = new StructType(new StructField[]{
                new StructField("date_time", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("project_name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("page_name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("page_count", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("page_size", DataTypes.IntegerType, true, Metadata.empty()),
        });
        return sparkSession.read().format("csv")
                .option("sep", ",")
                .option("header", "false")
                .schema(schema)
                .load(path);
    }

    private void saveAccurate() {
        try {
            FileWriter fileWriter = new FileWriter(new File(ACCURATE_SAVE_PATH));
            StringBuilder header = new StringBuilder("date");
            for (int i = 0; i < QUERIES.size(); i++) {
                header.append(",q").append(i + 1);
            }
            /*
            for (int i = 0; i < 8; i++) {
                header.append(",q").append(i + 1);
            }
            */
            header.append("\r\n");
            fileWriter.write(header.toString());
            accurateResults.forEach((time, results) -> {
                try {
                    StringBuilder body = new StringBuilder(time);
                    for (int i = 0; i < QUERIES.size(); i++) {
                        body.append(",").append(results.get(i).result);
                    }
                    /*
                    for (int i = 0; i < 8; i++) {
                        body.append(",").append(results.get(i).result);
                    }
                    */
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
    /*
    public void run() {
        try {
            for (int day = 1; day <= DAY_TOTAL; day++) {
                for (int hour = 0; hour < 24; hour++) {
                    Dataset<Row> batch = readBatch(day, hour);
                    String time = String.format("%02d%02d", day, hour);
                    String viewName = String.format("batch_%s", time);
                    AdaLogger.debug(this, "Handling " + time + "'s batch.");
                    batch.createTempView(viewName);
                    List<ResultUnit> results = Lists.newArrayList();
                    for (String query : QUERIES) {
                        Dataset<Row> result = sparkSession.sql(query.replaceAll("view_name", viewName));
                        results.add(new ResultUnit(day * 24 + hour - 23, (double) result.collectAsList().get(0).getLong(0)));
                    }
                    accurateResults.put(time, results);
                    sparkSession.catalog().dropTempView(String.format("batch_%02d%02d", day, hour));
                }
            }
            List<ResultUnit> previousResults = Lists.newArrayList();
            for (String ignored : QUERIES) {
                previousResults.add(new ResultUnit(0, 0.0));
            }
            for (int day = 1; day <= DAY_TOTAL; day++) {
                for (int hour = 0; hour < 24; hour++) {
                    String time = String.format("%02d%02d", day, hour);
                    int hours = day * 24 + hour - 23;
                    AdaLogger.debug(this, "Reducing " + time + "'s result.");
                    List<ResultUnit> singleResults = accurateResults.get(time);
                    List<ResultUnit> totalResults = Lists.newArrayList();
                    for (int i = 0; i < QUERIES.size(); i++) {
                        previousResults.get(i).resultPlus(singleResults.get(i).result);
                    }
                    for (int i = 0; i < QUERIES.size(); i++) {
                        if (i < 4) {
                            double result = previousResults.get(i).result / previousResults.get(i + 4).result;
                            totalResults.add(new ResultUnit(hours, result));
                        } else {
                            totalResults.add(new ResultUnit(hours, previousResults.get(i).result));
                        }
                    }
                    accurateResults.put(time, totalResults);
                }
            }
            saveAccurate();
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
    */
    public void run() {
        try {
            for (int day = 1; day <= DAY_TOTAL; day++) {
                for (int hour = 0; hour < 24; hour++) {
                    Dataset<Row> batch = readBatch(day, hour);
                    String time = String.format("%02d%02d", day, hour);
                    String viewName = String.format("batch_%s", time);
                    AdaLogger.debug(this, "Handling " + time + "'s batch.");
                    batch.createTempView(viewName);
                    List<ResultUnit> results = Lists.newArrayList();
                    for (String query : QUERIES) {
                        Dataset<Row> result = sparkSession.sql(query.replaceAll("view_name", viewName));
                        results.add(new ResultUnit(day * 24 + hour - 23, (double) result.collectAsList().get(0).getLong(0)));
                    }
                    accurateResults.put(time, results);
                    sparkSession.catalog().dropTempView(String.format("batch_%02d%02d", day, hour));
                }
            }
            List<ResultUnit> previousResults = Lists.newArrayList();
            /*
            for (int i = 0; i < 8; i++) {
                previousResults.add(new ResultUnit(0, 0.0));
            }
            */
            for (int i = 0; i < QUERIES.size(); i++) {
                previousResults.add(new ResultUnit(0, 0.0));
            }
            for (int day = 1; day <= DAY_TOTAL; day++) {
                for (int hour = 0; hour < 24; hour++) {
                    String time = String.format("%02d%02d", day, hour);
                    int hours = day * 24 + hour - 23;
                    AdaLogger.debug(this, "Reducing " + time + "'s result.");
                    List<ResultUnit> singleResults = accurateResults.get(time);
                    List<ResultUnit> totalResults = Lists.newArrayList();
                    /*
                    for (int i = 4; i < 8; i++) {
                        double totalAvg = (previousResults.get(i - 4).result + singleResults.get(i - 4).result)
                                / (previousResults.get(i + 4).result + singleResults.get(i + 4).result);
                        double previousAvg = previousResults.get(i - 4).result / previousResults.get(i + 4).result;
                        double singleAvg = singleResults.get(i - 4).result / singleResults.get(i + 4).result;
                        previousResults.get(i).result = (previousResults.get(i + 4).result * (previousResults.get(i).result + Math.pow(totalAvg - previousAvg, 2))
                                + singleResults.get(i + 4).result * (singleResults.get(i).result + Math.pow(totalAvg - singleAvg, 2)))
                                / (previousResults.get(i + 4).result + singleResults.get(i + 4).result);
                    }
                    */
                    for (int i = 0; i < QUERIES.size(); i++) {
                        if (QUERIES.get(i).contains("VAR_POP")) {
                            continue;
                        }
                        previousResults.get(i).resultPlus(singleResults.get(i).result);
                    }
                    for (int i = 0; i < QUERIES.size(); i++) {
                        if (QUERIES.get(i).contains("VAR_POP")) {
                            totalResults.add(new ResultUnit(hours, Math.pow(previousResults.get(i).result, 0.5)));
                        } else {
                            totalResults.add(new ResultUnit(hours, previousResults.get(i).result));
                        }
                    }
                    accurateResults.put(time, totalResults);
                }
            }
            saveAccurate();
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Exp1Accurate exp1Accurate = new Exp1Accurate();
        exp1Accurate.run();
    }
}
