package daslab.exp4;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class Exp4Accurate extends ExpTemplate {
    private final static List<String> QUERIES = ImmutableList.of(
            "SELECT AVG(page_count) FROM pagecounts",
            "SELECT AVG(page_count) FROM pagecounts WHERE page_size>80000",
            "SELECT AVG(page_count) FROM pagecounts WHERE project_name='aa'",
            "SELECT AVG(page_count) FROM pagecounts WHERE project_name='kk'"
    );

    private ExpResult expResult = new ExpResult();

    public Exp4Accurate() {
        this("Ada Exp4 - Accurate Result for Queries");
    }

    public Exp4Accurate(String name) {
        super(name);
    }

    @Override
    public void run() {
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        for (int i = ExpConfig.HOUR_START; i < ExpConfig.HOUR_TOTAL; i++) {
            int day = i / 24 + 1;
            int hour = i % 24;
            String time = String.format("%02d%02d", day, hour);
            append(day, hour);
            List<String> results = Lists.newLinkedList();
            for (String query : QUERIES) {
                double result = getSpark().sql(query).first().getDouble(0);
                results.add(String.format("%.8f", result));
            }
            AdaLogger.info(this, String.format("Accurate Result[%s]: {%s}", time, StringUtils.join(results, ", ")));
            expResult.addResult(time, results);
        }
        expResult.setHeader("time", "q1", "q2", "q3", "q4");
        save(expResult, "/tmp/ada/exp/exp4/exp4_accurate");
    }
}
