package daslab.exp5;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import daslab.bean.ExecutionReport;
import daslab.bean.Sample;
import daslab.bean.Sampling;
import daslab.context.AdaContext;
import daslab.exp.ExpConfig;
import daslab.exp.ExpResult;
import daslab.exp.ExpTemplate;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;

import java.util.*;

public class Exp5AdaTimeCost extends ExpTemplate {
    private final static int REPEAT_TIME = 1;
    private final static String RESULT_SAVE_PATH = "/tmp/ada/exp/exp5/ada_cost.csv";

    private final static List<String> QUERIES = ImmutableList.of(
            String.format("SELECT AVG(page_count) FROM %s.%s", ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE page_size>80000",  ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='aa'", ExpConfig.get("table.schema"), ExpConfig.get("table.name")),
            String.format("SELECT AVG(page_count) FROM %s.%s WHERE project_name='kk'",  ExpConfig.get("table.schema"), ExpConfig.get("table.name"))
    );

    public Exp5AdaTimeCost() {
        this("Ada Exp5 - Ada Time Cost on Stratified Sampling");
    }

    public Exp5AdaTimeCost(String name) {
        super(name);
    }

    @Override
    public void run() {
//        ExpResult expResult = new ExpResult(ImmutableList.of("time", "ada_cost", "strategy"));
        ExpResult expResult = new ExpResult();
        expResult.addHeader("time");
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

                for (Map.Entry<String, Object> entry : executionReport.search("sampling.cost").entrySet()) {
                    expResult.push(time, entry.getKey(), String.valueOf(entry.getValue()));
                }

//                expResult.push(time, String.valueOf(executionReport.getLong("sampling.cost.total")));
                expResult.push(time, "strategy", ((Map<Sample, Sampling>) executionReport.get("sampling.strategies"))
                        .entrySet()
                        .stream()
                        .map(e -> e.getKey().sampleType + ":" + e.getValue().toString() + ";")
                        .reduce((s1, s2) -> s1 + s2)
                        .orElse(""));
//                expResult.push(time, executionReport.getString("sampling.cost.pre-process"));
//                expResult.push(time, executionReport.getString("sampling.cost.sampling"));
//                expResult.push(time, executionReport.getString("sampling.cost.create-sample"));
                expResult.save(RESULT_SAVE_PATH);
            }
        }
        expResult.save(RESULT_SAVE_PATH);
    }
}
