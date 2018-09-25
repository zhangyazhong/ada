package daslab.exp5;

import com.google.common.collect.ImmutableList;
import daslab.context.AdaContext;
import daslab.exp.ExpQueryPool;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

/**
 * @author zyz
 * @version 2018-08-31
 */
public class Exp5AccurateResult extends ExpTemplate {
    public final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp5/(new)accurate_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    public Exp5AccurateResult() {
        this("Ada Exp5 - Accuracy Result");
    }

    public Exp5AccurateResult(String name) {
        super(name);
    }

    @Override
    public void run() {
        List<String> QUERIES = ExpQueryPool.QUERIES_EXCEPT(
                ImmutableList.of(
                        new ExpQueryPool.WhereClause("page_count"),
                        new ExpQueryPool.WhereClause("page_size")
                ), ImmutableList.of(
                        new ExpQueryPool.GroupByClause("project_name")
                ))
                .stream().map(ExpQueryPool.QueryString::toString).collect(Collectors.toList());
        QUERIES = ExpQueryPool.QUERIES_ONLY(
                new ExpQueryPool.WhereClause("page_size"),
                new ExpQueryPool.WhereClause("page_count")
        ).stream().map(ExpQueryPool.QueryString::toString).collect(Collectors.toList());
        ExpResult expResult = new ExpResult("time");
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        resetVerdict();
        AdaContext context = new AdaContext().start();
        for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
            int day = i / 24 + 1;
            int hour = i % 24;
            String time = String.format("%02d%02d", day, hour);
            String location = String.format(get("source.hdfs.location.pattern"), day, hour);
            AdaLogger.info(this, "Send a new batch at " + location);
            context.receive(location);
            runQueryBySpark(expResult, QUERIES, time);
            AdaLogger.info(this, String.format("Accurate Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ExpResult.SEPARATOR)));
            expResult.save(RESULT_SAVE_PATH);
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}
