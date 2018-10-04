package daslab.exp13;

import com.google.common.collect.ImmutableList;
import daslab.exp.ExpQueryPool;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

import static daslab.exp.ExpConfig.*;

/**
 * @author zyz
 * @version 2018-10-04
 */
public class Exp13BaseDataSpark extends ExpTemplate {
    public final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp13/accurate_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    public Exp13BaseDataSpark() {
        this("Ada Exp13 - Accuracy Result on 24h Data");
    }

    public Exp13BaseDataSpark(String name) {
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
        QUERIES.addAll(ExpQueryPool.QUERIES_ONLY(
                new ExpQueryPool.WhereClause("page_size"),
                new ExpQueryPool.WhereClause("page_count")
        ).stream().map(ExpQueryPool.QueryString::toString).collect(Collectors.toList()));
        QUERIES.addAll(ImmutableList.of(
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
        ));
        QUERIES.forEach(query -> AdaLogger.info(this, query));
        ExpResult expResult = new ExpResult("time");
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        resetVerdict();
        int day = HOUR_START / 24 + 1;
        int hour = HOUR_START % 24;
        String time = String.format("%02d%02d", day, hour);
        runQueryBySpark(expResult, QUERIES, time);
        AdaLogger.info(this, String.format("Accurate Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ExpResult.SEPARATOR)));
        expResult.save(RESULT_SAVE_PATH);
    }
}
