package daslab.exp7;

import com.google.common.collect.ImmutableList;
import daslab.context.AdaContext;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

public class Exp7AccurateResult extends ExpTemplate {
    public final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp7/accurate_result_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    public Exp7AccurateResult() {
        this("Ada Exp7 - Accuracy Result (TPC-H)");
    }

    public Exp7AccurateResult(String name) {
        super(name);
    }

    @Override
    public void run() {
        List<String> QUERIES;
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
        ExpResult expResult = new ExpResult("time");
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        resetVerdict();
        AdaContext context = new AdaContext().skipSampling(true).start();
        for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
            String[] locations = new String[HOUR_INTERVAL];
            String time = String.format("%02d~%02d", i, (i + HOUR_INTERVAL - 1));
            for (int j = i; j < i + HOUR_INTERVAL; j++) {
                locations[j - i] = String.format(get("source.hdfs.location.pattern"), j);
            }
            i = i + HOUR_INTERVAL - 1;
            AdaLogger.info(this, "Send a new batch at " + Arrays.toString(locations));
            context.receive(locations);
            runQueryBySpark(expResult, QUERIES, time);
            AdaLogger.info(this, String.format("Accurate Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ExpResult.SEPARATOR)));
            expResult.save(RESULT_SAVE_PATH);
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}
