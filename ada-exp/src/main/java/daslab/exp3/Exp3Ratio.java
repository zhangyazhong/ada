package daslab.exp3;

import daslab.exp.ExpTemplate;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;

public class Exp3Ratio extends ExpTemplate {
    private static int[] EXP_RATIO = {1, 2, 3, 5, 10, 20, 30, 50};
    private long[] cost;
    private final static int TIMES = 10;
    private final static int DATA = 50000_0000;

    public Exp3Ratio() {
        super("Ada Exp3 - Exp3 Stratified");
        cost = new long[EXP_RATIO.length];
        for (int i = 0; i < cost.length; i++) {
            cost[i] = 0;
        }
    }

    @Override
    public void run() {
        execute("DROP DATABASE IF EXISTS wiki_exp3 CASCADE").execute("CREATE DATABASE wiki_exp3");
        execute("CREATE TABLE wiki_exp3.pagecounts LIKE wiki_ada.pagecounts");
        execute("INSERT INTO wiki_exp3.pagecounts SELECT * FROM wiki_ada.pagecounts LIMIT " + DATA);
        for (int time = 0; time < TIMES; time++) {
            try {
                getVerdict().execute("DROP SAMPLES OF wiki_exp3");
                for (int index = 0; index < EXP_RATIO.length; index++) {
                    int ratio = EXP_RATIO[index];
                    long count = getSpark().sql("SELECT COUNT(*) AS count FROM wiki_exp3.pagecounts").toDF().collectAsList().get(0).getLong(0);
                    AdaLogger.info(this, "Table cardinality is: " + count);
                    Thread.sleep(1000);
                    Long startTime = System.currentTimeMillis();
                    getVerdict().execute("CREATE " + ratio + "% UNIFORM SAMPLE OF wiki_exp3.pagecounts ON project_name");
                    Long finishTime = System.currentTimeMillis();
                    cost[index] = cost[index] + finishTime - startTime;
                    String samplingTime = String.format("%d:%02d.%03d", (finishTime - startTime) / 60000, ((finishTime - startTime) / 1000) % 60, (finishTime - startTime) % 1000);
                    AdaLogger.info(this, "Data[" + DATA + "] with stratified ratio[" + ratio + "%] cost: " + samplingTime);
                }
            } catch (VerdictException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        execute("DROP DATABASE IF EXISTS wiki_exp3 CASCADE");
        output();
    }

    private void output() {
        for (int i = 0; i < cost.length; i++) {
            long costTime = cost[i] / TIMES;
            String samplingTime = String.format("%d:%02d.%03d", costTime / 60000, (costTime / 1000) % 60, costTime % 1000);
            AdaLogger.info(this, String.format("No.%d data scale[%d] with stratified ratio[%d%%] cost: %s", i, DATA, EXP_RATIO[i], samplingTime));
        }
    }

    public static void main(String[] args) {
        ExpTemplate exp = new Exp3Ratio();
        exp.run();
    }
}
