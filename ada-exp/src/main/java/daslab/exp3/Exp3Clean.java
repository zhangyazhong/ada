package daslab.exp3;

import daslab.utils.AdaLogger;
import org.apache.spark.sql.SparkSession;

public class Exp3Clean {
    private SparkSession sparkSession;

    public Exp3Clean() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - Exp3")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                .config("spark.executor.memory", "12g")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
    }

    public void run() {
        execute("DROP DATABASE IF EXISTS wiki_exp3 CASCADE");
    }

    private Exp3Clean execute(String sql) {
        AdaLogger.debug(this, "About to run: " + sql);
        sparkSession.sql(sql);
        return this;
    }

    public static void main(String[] args) {
        Exp3Clean exp3Clean = new Exp3Clean();
        exp3Clean.run();
    }
}
