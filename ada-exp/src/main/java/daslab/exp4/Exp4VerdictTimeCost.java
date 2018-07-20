package daslab.exp4;

import com.google.common.collect.ImmutableList;
import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import daslab.utils.AdaTimer;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;

import java.util.List;

public class Exp4VerdictTimeCost extends ExpTemplate {
    private final static int SAMPLE_COUNT = 1;

    private final static List<String> QUERIES = ImmutableList.of(
            String.format("SELECT AVG(page_count) FROM %s.%s", ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE page_size>80000",  ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='aa'", ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='kk'",  ExpConfig.get("table.schema"), ExpConfig.get("table.name"))
    );

    public Exp4VerdictTimeCost() {
        this("Ada Exp4 - Verdict Sampling Time Cost");
    }

    public Exp4VerdictTimeCost(String name) {
        super(name);
    }

    @Override
    public void run() {
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        ExpResult expResult = new ExpResult(ImmutableList.of("time", "verdict(ms)", "q0", "q1", "q2", "q3"));
        try {
            getVerdict().execute(String.format("DROP SAMPLES OF %s.%s", ExpConfig.get("table.schema"), ExpConfig.get("table.name")));
            for (int i = ExpConfig.HOUR_START; i < ExpConfig.HOUR_TOTAL; i++) {
                int day = i / 24 + 1;
                int hour = i % 24;
                String time = String.format("%02d%02d", day, hour);
                append(day, hour);
                for (int j = 0; j < SAMPLE_COUNT; j++) {
                    String sampling = String.format("CREATE 10%% UNIFORM SAMPLE OF %s.%s", ExpConfig.get("table.schema"), ExpConfig.get("table.name"));
                    long start = System.currentTimeMillis();
                    getVerdict().execute(sampling);
                    long finish = System.currentTimeMillis();
                    long cost = finish - start;
                    expResult.addResult(time, String.valueOf(cost));
                    for (String QUERY : QUERIES) {
                        try {
                            AdaTimer.start();
                            Row row = getVerdict().sql(QUERY).first();
                            double avg = row.getDouble(0);
                            double err = row.getDouble(1);
                            expResult.addResult(time, String.valueOf(AdaTimer.stop()));
                        } catch (VerdictException e) {
                            e.printStackTrace();
                        }
                    }
                    sampling = String.format("DROP SAMPLES OF %s.%s", ExpConfig.get("table.schema"), ExpConfig.get("table.name"));
                    getVerdict().execute(sampling);
                }
                AdaLogger.info(this, String.format("Verdict Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ", ")));
            }
        }
        catch (VerdictException e) {
            e.printStackTrace();
        }
        save(expResult, "/tmp/ada/exp/exp4/exp4_verdict_cost");
    }
}
