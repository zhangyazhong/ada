package daslab.exp;

import daslab.utils.AdaLogger;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.spark.sql.SparkSession;

public abstract class ExpTemplate {
    private static SparkSession sparkSession;
    private static VerdictSpark2Context verdictSpark2Context;

    public ExpTemplate(String name) {
        if (sparkSession == null) {
            sparkSession = SparkSession
                    .builder()
                    .appName(name)
                    .enableHiveSupport()
                    .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                    .config("spark.executor.memory", "16g")
                    .config("spark.driver.memory", "4g")
                    .getOrCreate();
            sparkSession.sparkContext().setLogLevel("ERROR");
        } else {
            sparkSession.conf().set("spark.app.name", "name");
        }
        try {
            if (verdictSpark2Context == null) {
                verdictSpark2Context = new VerdictSpark2Context(sparkSession.sparkContext());
                verdictSpark2Context.sql("USE wiki_ada");
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
}
