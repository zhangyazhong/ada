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

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

/**
 * @author zyz
 * @version 2018-10-29
 */
public class Exp20VerdictResult extends ExpTemplate {
    private final static int REPEAT_TIME = 10;
    public final static String VERDICT_RESULT_PATH = String.format("/tmp/ada/exp/exp20/verdict_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    private static List<String> QUERIES = ImmutableList.of(
            String.format("SELECT SUM(page_count) FROM %s.%s WHERE project_name='kk'", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name"))
    );

    public Exp20VerdictResult() {
        this("Ada Exp20 - Verdict Result Accuracy Test");
    }

    public Exp20VerdictResult(String name) {
        super(name);
    }

    @Override
    public void run() {
        try {
            ExpResult verdictResult = new ExpResult("time");
            for (int k = 0; k < REPEAT_TIME; k++) {
                SystemRestore.restoreModules().forEach(RestoreModule::restore);
                AdaLogger.info(this, "Restored database.");
                resetVerdict();
                AdaContext context = new AdaContext().start().enableForceResample(true);
                runQueryByVerdict(verdictResult, QUERIES, String.format("0000~%02d%02d", (HOUR_START - 1) / 24 + 1, (HOUR_START - 1) % 24), k);
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
                    runQueryByVerdict(verdictResult, QUERIES, time, k);
                    AdaLogger.info(this, String.format("Verdict Result[%s]: {%s}", time, StringUtils.join(verdictResult.getColumns(time), ExpResult.SEPARATOR)));
                    verdictResult.save(VERDICT_RESULT_PATH);
                }
                verdictResult.save(VERDICT_RESULT_PATH);
            }
            verdictResult.save(VERDICT_RESULT_PATH);
        }
        catch (VerdictException e) {
            e.printStackTrace();
        }
    }
}