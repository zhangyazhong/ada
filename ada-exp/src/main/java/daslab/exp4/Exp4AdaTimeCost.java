package daslab.exp4;

import com.google.common.collect.ImmutableList;
import daslab.bean.ExecutionReport;
import daslab.context.AdaContext;
import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import edu.umich.verdict.exceptions.VerdictException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;

import java.util.List;

public class Exp4AdaTimeCost extends ExpTemplate {
    private final static int REPEAT_TIME = 1;

    private final static List<String> QUERIES = ImmutableList.of(
            String.format("SELECT AVG(page_count) FROM %s.%s", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE page_size>80000",  ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='aa'", ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='kk'",  ExpConfig.get("data.table.schema"), ExpConfig.get("data.table.name"))
    );

    public Exp4AdaTimeCost() {
        this("Ada Exp4 - Ada Sampling Time Cost");
    }

    public Exp4AdaTimeCost(String name) {
        super(name);
    }

    @Override
    public void run() {
        ExpResult expResult = new ExpResult(ImmutableList.of("time", "ada_total", "ada_pre-process", "ada_sampling", "ada_create-sample", "q0", "q1", "q2", "q3"));
        for (int k = 0; k < REPEAT_TIME; k++) {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            resetVerdict();
            AdaContext context = new AdaContext();
            context.start();
            for (int i = ExpConfig.HOUR_START; i < ExpConfig.HOUR_TOTAL; i++) {
                int day = i / 24 + 1;
                int hour = i % 24;
                String time = String.format("%02d%02d", day, hour);
                String location = String.format(get("source.hdfs.location.pattern"), day, hour);
                AdaLogger.info(this, "Send a new batch at " + location);
                ExecutionReport executionReport = context.receive(location);
                expResult.addResult(time, executionReport.getString("sampling.method") + "/" + String.valueOf(executionReport.getLong("sampling.cost.total")));
                expResult.addResult(time, executionReport.getString("sampling.cost.pre-process"));
                expResult.addResult(time, executionReport.getString("sampling.cost.sampling"));
                expResult.push(time, executionReport.getString("sampling.cost.create-sample"));
                AdaLogger.info(this, String.format("Ada Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ", ")));
                for (String QUERY : QUERIES) {
                    try {
                        long start = System.currentTimeMillis();
                        Row row = getVerdict().sql(QUERY).first();
                        double avg = row.getDouble(0);
                        double err = row.getDouble(1);
                        long finish = System.currentTimeMillis();
                        expResult.push(time, String.valueOf(finish - start));
                    } catch (VerdictException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        save(expResult, "/tmp/ada/exp/exp4/exp4_ada_cost.csv");
    }
}
