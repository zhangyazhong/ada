package daslab.exp20;

import com.google.common.collect.ImmutableList;
import daslab.context.AdaContext;
import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

import static daslab.exp.ExpConfig.*;

/**
 * @author zyz
 * @version 2018-10-29
 */
public class Exp20AdaResult extends ExpTemplate {
    private final static int REPEAT_TIME = 10;
    public final static String ADA_RESULT_PATH = String.format("/tmp/ada/exp/exp20/ada_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);
    public final static String SPARK_RESULT_PATH = String.format("/tmp/ada/exp/exp20/accurate_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    private static List<String> QUERIES = ImmutableList.of(
            String.format("SELECT SUM(page_count) FROM %s.%s WHERE project_name='kk'", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name"))
    );

    public Exp20AdaResult() {
        this("Ada Exp20 - Ada Result Accuracy Test");
    }

    public Exp20AdaResult(String name) {
        super(name);
    }

    @Override
    public void run() {
        try {
            ExpResult adaResult = new ExpResult("time");
            ExpResult sparkResult = new ExpResult("time");
            for (int k = 0; k < REPEAT_TIME; k++) {
                SystemRestore.restoreModules().forEach(RestoreModule::restore);
                AdaLogger.info(this, "Restored database.");
                resetVerdict();
                AdaContext context = new AdaContext().start();
                runQueryByVerdict(adaResult, QUERIES, String.format("0000~%02d%02d", (HOUR_START - 1) / 24 + 1, (HOUR_START - 1) % 24), k);
                if (k < 1) {
                    runQueryBySpark(sparkResult, QUERIES, String.format("0000~%02d%02d", (HOUR_START - 1) / 24 + 1, (HOUR_START - 1) % 24));
                    AdaLogger.info(this, String.format("Accurate Result[%s]: {%s}", String.format("0000~%02d%02d", (HOUR_START - 1) / 24 + 1, (HOUR_START - 1) % 24), StringUtils.join(sparkResult.getColumns(String.format("0000~%02d%02d", (HOUR_START - 1) / 24 + 1, (HOUR_START - 1) % 24)), ExpResult.SEPARATOR)));
                    sparkResult.save(SPARK_RESULT_PATH);
                }
                for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
                    String[] locations = new String[HOUR_INTERVAL];
                    String time = String.format("%02d%02d~%02d%02d",
                            i / 24 + 1, i % 24, (i + HOUR_INTERVAL - 1) / 24 + 1, (i + HOUR_INTERVAL - 1) % 24);
                    for (int j = 0; j < HOUR_INTERVAL; j++) {
                        int day = (i + j) / 24 + 1;
                        int hour = (i + j) % 24;
                        locations[j] = String.format(get("source.hdfs.location.pattern"), day, hour);
                    }
                    i = i + HOUR_INTERVAL - 1;
                    AdaLogger.info(this, "Send a new batch at " + Arrays.toString(locations));
                    context.receive(locations);
                    runQueryByVerdict(adaResult, QUERIES, time, k);
                    AdaLogger.info(this, String.format("Ada Result[%s]: {%s}", time, StringUtils.join(adaResult.getColumns(time), ExpResult.SEPARATOR)));
                    adaResult.save(ADA_RESULT_PATH);
                    if (k < 1) {
                        runQueryBySpark(sparkResult, QUERIES, time);
                        AdaLogger.info(this, String.format("Accurate Result[%s]: {%s}", time, StringUtils.join(sparkResult.getColumns(time), ExpResult.SEPARATOR)));
                        sparkResult.save(SPARK_RESULT_PATH);
                    }
                }
                adaResult.save(ADA_RESULT_PATH);
            }
            adaResult.save(ADA_RESULT_PATH);
        }
        catch (VerdictException e) {
            e.printStackTrace();
        }
    }
}
