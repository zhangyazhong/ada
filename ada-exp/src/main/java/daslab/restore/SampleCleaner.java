package daslab.restore;

import daslab.exp.ExpConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SampleCleaner implements RestoreModule {

    private SparkSession sparkSession;

    public SampleCleaner() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - DatabaseRestore")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                .config("spark.executor.memory", ExpConfig.SPARK_EXECUTOR_MEMORY)
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
    }

    @Override
    public void restore() {
        List<Row> databases = sparkSession.sql("SHOW DATABASES").collectAsList();
        for (Row database : databases) {
            String name = database.getString(0);
            if (name.contains("verdict")) {
                sparkSession.sql("DROP DATABASE " + name + " CASCADE");
            }
        }
    }

    public static void main(String[] args) {
        RestoreModule r = new SampleCleaner();
        r.restore();
    }
}
