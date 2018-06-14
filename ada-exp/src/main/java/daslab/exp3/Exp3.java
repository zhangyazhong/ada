package daslab.exp3;

import daslab.utils.AdaLogger;
import edu.umich.verdict.VerdictSpark2Context;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.spark.sql.SparkSession;

public class Exp3 {
    private SparkSession sparkSession;
    private VerdictSpark2Context verdictSpark2Context;

    private static long[] EXP_DATA =
            {1000_0000, 2000_0000, 3000_0000, 4000_0000, 5000_0000,
                    6000_0000, 7000_0000, 8000_0000, 9000_0000, 10000_0000};
    private final static int SAMPLE_RATIO = 10;

    public Exp3() {
        sparkSession = SparkSession
                .builder()
                .appName("Ada Exp - Exp3")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "hdfs://master:9000/home/hadoop/spark/")
                .config("spark.executor.memory", "12g")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        try {
            verdictSpark2Context = new VerdictSpark2Context(sparkSession.sparkContext());
            verdictSpark2Context.sql("USE wiki_ada");
        } catch (VerdictException e) {
            e.printStackTrace();
        }
        EXP_DATA = new long[10];
        for (int i = 0; i < 10; i++) {
            EXP_DATA[i] = (i + 1) * 5000_0000;
        }
    }

    public void run() {
//        SystemRestore.restoreModules().forEach(RestoreModule::restore);
//        AdaLogger.info(this, "Restored database.");
        execute("DROP DATABASE IF EXISTS wiki_exp3 CASCADE").execute("CREATE DATABASE wiki_exp3");
        execute("CREATE TABLE wiki_exp3.pagecounts LIKE wiki_ada.pagecounts");
        for (long data : EXP_DATA) {
            execute("INSERT INTO wiki_exp3.pagecounts SELECT * FROM wiki_ada.pagecounts LIMIT " + data);
            long count = sparkSession.sql("SELECT COUNT(*) AS count FROM wiki_exp3.pagecounts").toDF().collectAsList().get(0).getLong(0);
            AdaLogger.info(this, "Table cardinality is: " + count);
            try {
                verdictSpark2Context.execute("DROP SAMPLES OF wiki_exp3");
                Thread.sleep(1000);
                Long startTime = System.currentTimeMillis();
                verdictSpark2Context.execute("CREATE " + SAMPLE_RATIO + "% SAMPLE OF wiki_exp3.pagecounts");
                Long finishTime = System.currentTimeMillis();
                String samplingTime = String.format("%d:%02d.%03d", (finishTime - startTime) / 60000, ((finishTime - startTime) / 1000) % 60, (finishTime - startTime) % 1000);
                AdaLogger.info(this, "Data[" + data + "] with ratio[" + SAMPLE_RATIO + "%] cost: " + samplingTime);
            } catch (VerdictException | InterruptedException e) {
                e.printStackTrace();
            }
            execute("TRUNCATE TABLE wiki_exp3.pagecounts");
        }
    }

    private Exp3 execute(String sql) {
        AdaLogger.debug(this, "About to run: " + sql);
        sparkSession.sql(sql);
        return this;
    }

    public static void main(String[] args) {
        Exp3 exp3 = new Exp3();
        exp3.run();
    }
}
