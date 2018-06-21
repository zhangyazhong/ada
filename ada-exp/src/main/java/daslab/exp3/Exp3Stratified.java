package daslab.exp3;

import daslab.exp.ExpTemplate;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;

public class Exp3Stratified extends ExpTemplate {
    private static long[] EXP_DATA =
            {1000_0000, 2000_0000, 3000_0000, 4000_0000, 5000_0000,
                    6000_0000, 7000_0000, 8000_0000, 9000_0000, 10000_0000};
    private long[] cost;
    private final static int SAMPLE_RATIO = 10;
    private final static int TIMES = 10;

    public Exp3Stratified() {
        super("Ada Exp3 - Exp3 Stratified");
        EXP_DATA = new long[11];
        EXP_DATA[0] = 5000_0000;
        for (int i = 1; i < EXP_DATA.length; i++) {
            EXP_DATA[i] = i * 5000_0000;
        }
        cost = new long[EXP_DATA.length];
        for (int i = 0; i < cost.length; i++) {
            cost[i] = 0;
        }
    }

    @Override
    public void run() {
        for (int time = 0; time < TIMES; time++) {
            execute("DROP DATABASE IF EXISTS wiki_exp3 CASCADE").execute("CREATE DATABASE wiki_exp3");
            execute("CREATE TABLE wiki_exp3.pagecounts LIKE wiki_ada.pagecounts");
            for (int index = 0; index < EXP_DATA.length; index++) {
                long data = EXP_DATA[index];
                execute("INSERT INTO wiki_exp3.pagecounts SELECT * FROM wiki_ada.pagecounts LIMIT " + data);
                long count = getSpark().sql("SELECT COUNT(*) AS count FROM wiki_exp3.pagecounts").toDF().collectAsList().get(0).getLong(0);
                AdaLogger.info(this, "Table cardinality is: " + count);
                try {
                    getVerdict().execute("DROP SAMPLES OF wiki_exp3");
                    Thread.sleep(1000);
                    Long startTime = System.currentTimeMillis();
                    getVerdict().execute("CREATE " + SAMPLE_RATIO + "% STRATIFIED SAMPLE OF wiki_exp3.pagecounts ON project_name");
                    Long finishTime = System.currentTimeMillis();
                    cost[index] = cost[index] + finishTime - startTime;
                    String samplingTime = String.format("%d:%02d.%03d", (finishTime - startTime) / 60000, ((finishTime - startTime) / 1000) % 60, (finishTime - startTime) % 1000);
                    AdaLogger.info(this, "Data[" + data + "] with stratified ratio[" + SAMPLE_RATIO + "%] cost: " + samplingTime);
                } catch (VerdictException | InterruptedException e) {
                    e.printStackTrace();
                }
                execute("TRUNCATE TABLE wiki_exp3.pagecounts");
            }
            execute("DROP DATABASE IF EXISTS wiki_exp3 CASCADE");
        }
        output();
    }

    private void output() {
        for (int i = 0; i < cost.length; i++) {
            long costTime = cost[i] / TIMES;
            String samplingTime = String.format("%d:%02d.%03d", costTime / 60000, (costTime / 1000) % 60, costTime % 1000);
            AdaLogger.info(this, String.format("No.%d data scale[%d] with stratified ratio[%d%%] cost: %s", i, EXP_DATA[i], SAMPLE_RATIO, samplingTime));
        }
    }

    public static void main(String[] args) {
        ExpTemplate exp = new Exp3Uniform();
        exp.run();
    }
}
