package daslab.exp5;

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

import java.util.List;

/**
 * @author zyz
 * @version 2018-08-08
 */
public class Exp5VerdictResult extends ExpTemplate {
    private final static int REPEAT_TIME = 10;
    private final static String RESULT_SAVE_PATH = "/tmp/ada/exp/exp5/verdict_result.csv";

    private final static List<String> QUERIES = ImmutableList.of(
            // huge number group
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='uk'", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            // very small group
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='www'", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            // common group
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='aa'", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            // high variety group
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='kk'",  ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name"))
    );

    public Exp5VerdictResult() {
        this("Ada Exp5 - Verdict Result Accuracy Test");
    }

    public Exp5VerdictResult(String name) {
        super(name);
    }

    @Override
    public void run() {
        ExpResult expResult = new ExpResult("time");
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            try {
                getVerdict().sql(String.format("DROP %s%% UNIFORM SAMPLES OF %s.%s", get("sample.init.ratio"), get("data.table.schema"), get("data.table.name")));
            } catch (VerdictException e) {
                e.printStackTrace();
            }
            AdaContext context = new AdaContext().start(true);
            for (int i = ExpConfig.HOUR_START; i < ExpConfig.HOUR_TOTAL; i++) {
                int day = i / 24 + 1;
                int hour = i % 24;
                String time = String.format("%02d%02d", day, hour);
                String location = String.format(get("source.hdfs.location.pattern"), day, hour);
                AdaLogger.info(this, "Send a new batch at " + location);
                context.receive(location);
                try {
                    runQuery(expResult, QUERIES, time, k);
                } catch (VerdictException e) {
                    e.printStackTrace();
                }
                AdaLogger.info(this, String.format("Verdict Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ", ")));
            }
            expResult.save(RESULT_SAVE_PATH);
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}
