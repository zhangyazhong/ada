package daslab.exp;

import daslab.utils.AdaLogger;
import daslab.utils.AdaSystem;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public abstract class ExpTemplate implements ExpRunnable {
    private static SparkSession sparkSession;
    private static VerdictSpark2Context verdictSpark2Context;

    public ExpTemplate(String name) {
        if (sparkSession == null) {
            sparkSession = SparkSession
                    .builder()
                    .appName(name)
                    .enableHiveSupport()
                    .config("spark.sql.warehouse.dir", get("spark.sql.warehouse.dir"))
                    .config("spark.executor.memory", get("spark.executor.memory"))
                    .config("spark.driver.memory", get("spark.driver.memory"))
                    .getOrCreate();
            sparkSession.sparkContext().setLogLevel("ERROR");
        } else {
            sparkSession.conf().set("spark.app.name", name);
        }
        try {
            verdictSpark2Context = new VerdictSpark2Context(sparkSession.sparkContext());
            verdictSpark2Context.sql("USE " + get("data.table.schema"));
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    public String get(String key) {
        return ExpConfig.get(key);
    }

    public void resetVerdict() {
        try {
            verdictSpark2Context = new VerdictSpark2Context(sparkSession.sparkContext());
            verdictSpark2Context.sql("USE " + get("data.table.schema"));
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

    @Override
    public abstract void run();

    public void append(String schema, String table, String path) {
        String command = "hadoop fs -cp " + path + " " + get("data.table.hdfs.location");
        AdaLogger.debug(this, "Loading " + path + " into table");
        AdaSystem.call(command);
    }

    public void append(String schema, String table, int day, int hour) {
        String path = String.format(get("source.hdfs.location"), day, hour);
        append(schema, table, path);
    }

    public void append(int day, int hour) {
        append(get("data.table.schema"), get("data.table.name"), day, hour);
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

    public void runQuery(ExpResult expResult, List<String> queries, String time, int repeatNo) throws VerdictException {
        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);
            Row row = getVerdict().sql(query).first();
            double avg = row.getDouble(0);
            double err = row.getDouble(1);
            expResult.push(time, "q" + i + "_" + repeatNo, String.format("%.8f/%.8f", avg, err));
        }
    }
}
