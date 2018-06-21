package daslab.restore;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SampleRestore implements RestoreModule {

    private SparkSession sparkSession;

    public SampleRestore() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - DatabaseRestore")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                .config("spark.executor.memory", "12g")
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
        RestoreModule r = new SampleRestore();
        r.restore();
    }
}
