package daslab.exp4;

import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;

public class Exp4VerdictTimeCost extends ExpTemplate {
    private final static int SAMPLE_COUNT = 1;

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
        ExpResult expResult = new ExpResult(Exp4Verdict.generateHeader());
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
