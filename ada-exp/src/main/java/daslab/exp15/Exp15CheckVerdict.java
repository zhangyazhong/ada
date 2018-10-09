package daslab.exp15;

import daslab.bean.ExecutionReport;
import daslab.context.AdaContext;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

public class Exp15CheckVerdict extends ExpTemplate {
    private final static int REPEAT_TIME = 1;
    public final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp15/verdict_check_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    public Exp15CheckVerdict() {
        this("Ada Exp15 - Check Verdict Approximate Result");
    }

    public Exp15CheckVerdict(String name) {
        super(name);
    }

    @Override
    public void run() {
        /*
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
        */
        final String QUERY = "SELECT COUNT(page_count) FROM %s.%s WHERE project_name='kk'";
        final String group = "SELECT verdict_vprob FROM wiki_ada_verdict.%s WHERE project_name='kk' LIMIT 1";
        final String all = "SELECT samplingratio FROM wiki_ada_verdict.verdict_meta_name";
        ExpResult expResult = new ExpResult("time");
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            AdaContext context = new AdaContext()
                    .enableForceResample(true)
                    .enableDebug(true)
                    .start();
            for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
                int day = i / 24 + 1;
                int hour = i % 24;
                String time = String.format("%02d%02d", day, hour);
                String location = String.format(get("source.hdfs.location.pattern"), day, hour);
                AdaLogger.info(this, "Send a new batch at " + location);
                ExecutionReport report = context.receive(location);
                try {
                    JSONArray accurateResult = runQueryBySpark(String.format(QUERY, "wiki_ada", "pagecounts"));
                    JSONArray verdictResult = runQueryByVerdict(String.format(QUERY, "wiki_ada", "pagecounts"));
                    JSONArray sparkResult = runQueryBySpark(String.format(QUERY, "wiki_ada_verdict", report.get("sample.table")));
                    JSONArray rKK = runQueryBySpark(String.format(group, report.get("sample.table")));
                    JSONArray rALL = runQueryBySpark(all);
                    expResult.push(time, "accurate", accurateResult.toString())
                            .push(time, "verdict", verdictResult.toString())
                            .push(time, "sample", sparkResult.toString())
                            .push(time, "r_kk", rKK.toString())
                            .push(time, "r_all", rALL.toString());
                } catch (VerdictException e) {
                    e.printStackTrace();
                }
                AdaLogger.info(this, String.format("Ada Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ExpResult.SEPARATOR)));
                expResult.save(RESULT_SAVE_PATH);
            }
            expResult.save(RESULT_SAVE_PATH);
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}
