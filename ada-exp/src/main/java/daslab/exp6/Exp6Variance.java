package daslab.exp6;

import daslab.bean.ExecutionReport;
import daslab.context.AdaContext;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;

import java.util.Arrays;
import java.util.Map;

import static daslab.exp.ExpConfig.HOUR_INTERVAL;
import static daslab.exp.ExpConfig.HOUR_START;
import static daslab.exp.ExpConfig.HOUR_TOTAL;

@SuppressWarnings("Duplicates")
public class Exp6Variance extends ExpTemplate {
    private final static int REPEAT_TIME = 1;
    private final static String RESULT_SAVE_PATH = String.format("/tmp/ada/exp/exp6/variance_%d_%d_%d.csv", HOUR_START, HOUR_TOTAL, HOUR_INTERVAL);

    public Exp6Variance() {
        this("Ada Exp6 - Database Variance");
    }

    public Exp6Variance(String name) {
        super(name);
    }

    @Override
    public void run() {
        ExpResult expResult = new ExpResult();
        expResult.addHeader("time");
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            AdaContext context = new AdaContext().start().skipSampling(true);
            for (int i = HOUR_START; i < HOUR_TOTAL; i++) {
                String[] locations = new String[HOUR_INTERVAL];
                String time = String.format("%02d%02d~%02d%02d",
                        i / 24 + 1, i % 24, (i + HOUR_INTERVAL - 1) / 24 + 1, (i + HOUR_INTERVAL - 1) % 24);
                for (int j = 0; j < HOUR_INTERVAL; j++) {
                    int day = (i + j) / 24 + 1;
                    int hour = (i + j) % 24;
                    locations[j] = String.format(get("source.hdfs.location.pattern"), day, hour);
                }
                i = i + HOUR_INTERVAL - 1;
                AdaLogger.info(this, "Send a new batch at " + Arrays.toString(locations));
                ExecutionReport executionReport = context.receive(locations);
                for (Map.Entry<String, Object> entry : executionReport.search("table.variance").entrySet()) {
                    expResult.push(time, entry.getKey(), String.valueOf(entry.getValue()));
                }
                expResult.save(RESULT_SAVE_PATH);
            }
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}