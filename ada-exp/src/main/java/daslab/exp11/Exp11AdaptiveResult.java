package daslab.exp11;

import com.google.common.collect.ImmutableList;
import daslab.context.AdaContext;
import daslab.exp.ExpQueryPool;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

@SuppressWarnings("Duplicates")
public class Exp11AdaptiveResult extends ExpTemplate {
    private final static int REPEAT_TIME = 10;
    public final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp10/(new)adaptive_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    private static List<String> QUERIES;

    public Exp11AdaptiveResult() {
        this("Ada Exp11 - Adaptive Result Accuracy Test");
    }

    public Exp11AdaptiveResult(String name) {
        super(name);
    }

    @Override
    public void run() {
        QUERIES = ExpQueryPool.QUERIES_EXCEPT(
                ImmutableList.of(
                        new ExpQueryPool.WhereClause("page_count"),
                        new ExpQueryPool.WhereClause("page_size")
                ), ImmutableList.of(
                        new ExpQueryPool.GroupByClause("project_name")
                ))
                .stream().map(ExpQueryPool.QueryString::toString).collect(Collectors.toList());
        /*
        QUERIES = ExpQueryPool.QUERIES_ONLY(
                new ExpQueryPool.WhereClause("page_size"),
                new ExpQueryPool.WhereClause("page_count")
        ).stream().map(ExpQueryPool.QueryString::toString).collect(Collectors.toList());
        */
        /*
        QUERIES = ImmutableList.of(
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=3",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=4",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=5",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=6",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=7",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=8",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=9",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=10",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=3",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=4",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=5",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=6",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=7",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=8",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=9",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=10"
        );
        */
        ExpResult expResult = new ExpResult("time");
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            AdaContext context = new AdaContext().start().enableAdaptive(true);
            for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
                int day = i / 24 + 1;
                int hour = i % 24;
                String time = String.format("%02d%02d", day, hour);
                String location = String.format(get("source.hdfs.location.pattern"), day, hour);
                AdaLogger.info(this, "Send a new batch at " + location);
                context.receive(location);
                try {
                    runQueryByVerdict(expResult, QUERIES, time, k);
                } catch (VerdictException e) {
                    e.printStackTrace();
                }
                AdaLogger.info(this, String.format("Adaptive Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ExpResult.SEPARATOR)));
                expResult.save(RESULT_SAVE_PATH);
            }
            expResult.save(RESULT_SAVE_PATH);
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}

