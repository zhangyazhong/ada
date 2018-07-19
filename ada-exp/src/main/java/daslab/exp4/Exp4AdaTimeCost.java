package daslab.exp4;

import daslab.bean.ExecutionReport;
import daslab.context.AdaContext;
import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;
import org.apache.commons.lang.StringUtils;

public class Exp4AdaTimeCost extends ExpTemplate {
    private final static int REPEAT_TIME = 1;

    public Exp4AdaTimeCost() {
        this("Ada Exp4 - Ada Sampling Time Cost");
    }

    public Exp4AdaTimeCost(String name) {
        super(name);
    }

    @Override
    public void run() {
        ExpResult expResult = new ExpResult(Exp4Verdict.generateHeader());
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
                String location = String.format("/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000", day, hour);
                AdaLogger.info(this, "Send a new batch at " + location);
                ExecutionReport executionReport = context.receive(location);
                expResult.addResult(time, executionReport.getString("sampling.method") + "/" + String.valueOf(executionReport.getLong("sampling.cost.total")));
                AdaLogger.info(this, String.format("Ada Result[%s]: {%s}", time, StringUtils.join(expResult.getColumns(time), ", ")));
            }
        }
        save(expResult, "/tmp/ada/exp/exp4/exp4_ada_cost");
    }
}
