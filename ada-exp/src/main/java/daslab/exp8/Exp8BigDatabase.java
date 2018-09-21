package daslab.exp8;

import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import daslab.utils.AdaTimer;
import edu.umich.verdict.exceptions.VerdictException;

/**
 * @author zyz
 * @version 2018-09-21
 */
public class Exp8BigDatabase extends ExpTemplate {
    public Exp8BigDatabase() {
        this("Ada Exp8 - Sampling on Huge Dataset");
    }

    public Exp8BigDatabase(String name) {
        super(name);
    }

    @Override
    public void run() {
        if (get("database.restore").trim().toLowerCase().equals("true")) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
        }
        ExpResult expResult = new ExpResult();
        expResult.addHeader("hour");
        for (int i = ExpConfig.HOUR_START; i < ExpConfig.HOUR_TOTAL; i++) {
            int day = i / 24 + 1;
            int hour = i % 24;
            append(day, hour);
        }
        try {
            resetVerdict();

            AdaTimer timer = AdaTimer.create();
            getVerdict().execute(String.format("CREATE 10%% UNIFORM SAMPLE OF %s.%s", "wiki_ada", "pagecounts"));
            expResult.push(String.valueOf(ExpConfig.HOUR_TOTAL), "10% un", String.valueOf(timer.stop()));

            timer = AdaTimer.create();
            getVerdict().execute(String.format("CREATE 10%% STRATIFIED SAMPLE OF %s.%s ON %s", "wiki_ada", "pagecounts", "project_name"));
            expResult.push(String.valueOf(ExpConfig.HOUR_TOTAL), "10% st(project_name)", String.valueOf(timer.stop()));
        } catch (VerdictException e) {
            e.printStackTrace();
        }
        expResult.save("/tmp/ada/exp/exp8/" + ExpConfig.HOUR_TOTAL + ".csv");
    }
}
