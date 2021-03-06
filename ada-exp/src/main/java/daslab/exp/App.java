package daslab.exp;

import daslab.exp1.Exp1;
import daslab.exp1.Exp1Cluster;
import daslab.exp1.Exp1Collect;
import daslab.exp1.Exp1Accurate;
import daslab.exp11.Exp11AdaTimeCost;
import daslab.exp11.Exp11AdaptiveResult;
import daslab.exp13.Exp13BaseDataSpark;
import daslab.exp13.Exp13BaseDataVerdict;
import daslab.exp15.Exp15CheckVerdict;
import daslab.exp16.Exp16StratifiedIncrement;
import daslab.exp19.Exp19VarianceIncrement;
import daslab.exp2.Exp2Core;
import daslab.exp2.Exp2Sender;
import daslab.exp20.Exp20AdaResult;
import daslab.exp20.Exp20VerdictResult;
import daslab.exp21.Exp21TimeCost;
import daslab.exp3.*;
import daslab.exp4.*;
import daslab.exp5.*;
import daslab.exp6.Exp6Variance;
import daslab.exp7.Exp7AccurateResult;
import daslab.exp7.Exp7AdaResult;
import daslab.exp7.Exp7AdaTimeCost;
import daslab.exp8.Exp8BigDatabase;
import daslab.exp8.Exp8CopyData;
import daslab.exp8.Exp8CreateTPCH;
import daslab.exp8.Exp8SplitTPCH;
import daslab.restore.RestoreModule;
import daslab.restore.SampleCleaner;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;

/**
 * The entrance of ada-exp module.
 * Map variety of commands to different execution goals.
 */
public class App {
    public App(String[] args) {
        if (args.length > 0) {
            switch (args[0].toLowerCase()) {
                case "restore":
                    AdaLogger.info(this, "Ada Exp operation: RESTORE");
                    SystemRestore.restoreModules().forEach(RestoreModule::restore);
                    break;
                case "clean_sample":
                    AdaLogger.info(this, "Ada Exp operation: Clean Sample");
                    new SampleCleaner().restore();
                    break;
                case "exp1":
                case "exp1_approximate":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 1 Only Approximate");
                    new Exp1().run();
                    break;
                case "exp1_cluster":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 1 on Lab Cluster");
                    new Exp1Cluster().run();
                    break;
                case "exp1_accurate":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 1 Only Accurate");
                    new Exp1Accurate().run();
                    break;
                case "exp1_collect":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 1 Collecting Results");
                    new Exp1Collect().run();
                    break;
                case "exp2":
                case "exp2_core":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 2 Core");
                    new Exp2Core().run();
                    break;
                case "exp2_sender":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 2 Sender");
                    new Exp2Sender().run();
                    break;
                case "exp3":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 3");
                    new Exp3().run();
                    break;
                case "exp3_with_restore":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 3 with Database Restore");
                    SystemRestore.restoreModules().forEach(RestoreModule::restore);
                    new Exp3().run();
                    break;
                case "exp3_clean":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 3 Only Clean Database");
                    new Exp3Clean().run();
                    break;
                case "exp3_uniform":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 3 Only Uniform Sample");
                    new Exp3Uniform().run();
                    break;
                case "exp3_stratified":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 3 Only Stratified Sample");
                    new Exp3Stratified().run();
                    break;
                case "exp3_ratio":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 3 with Different Ratio");
                    new Exp3Ratio().run();
                    break;
                case "exp4_accurate":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 4 for Accurate Result");
                    new Exp4Accurate().run();
                    break;
                case "exp4_verdict":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 4 for Verdict Result");
                    new Exp4Verdict().run();
                    break;
                case "exp4_ada":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 4 for Ada Result");
                    new Exp4Ada().run();
                    break;
                case "exp4_comparison":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 4 for Result Comparison");
                    new Exp4Comparison().run();
                    break;
                case "exp4_verdict_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 4 for Verdict Time Cost");
                    new Exp4VerdictTimeCost().run();
                    break;
                case "exp4_ada_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 4 for Ada Time Cost");
                    new Exp4AdaTimeCost().run();
                    break;
                case "exp5_ada_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Ada Time Cost");
                    new Exp5AdaTimeCost().run(args);
                    break;
                case "exp5_ada_cost_":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Ada Time Cost");
                    new Exp5AdaTimeCost().run(args);
                    break;
                case "exp5_verdict_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Verdict Time Cost");
                    new Exp5VerdictTimeCost().run(args);
                    break;
                case "exp5_verdict_cost_":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Verdict Time Cost");
                    new Exp5VerdictTimeCost().run(args);
                    break;
                case "exp5_ada_result":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Ada Result Performance");
                    new Exp5AdaResult().run();
                    break;
                case "exp5_verdict_result":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Verdict Result Performance");
                    new Exp5VerdictResult().run();
                    break;
                case "exp5_accurate_result":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Accurate Result Performance");
                    new Exp5AccurateResult().run();
                    break;
                case "exp5_result_compare":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Result Comparison");
                    new Exp5ResultComparison().run();
                    break;
                case "exp6_variance":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 6 for Database Variance");
                    new Exp6Variance().run();
                    break;
                case "exp7_ada_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 7 for Ada Time Cost (TPC-H)");
                    new Exp7AdaTimeCost().run();
                    break;
                case "exp7_accurate_result":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 7 for Accurate Result Performance (TPC-H)");
                    new Exp7AccurateResult().run(args);
                    break;
                case "exp7_ada_result":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 7 for Ada Result Performance (TPC-H)");
                    new Exp7AdaResult().run(args);
                    break;
                case "exp8_sampling":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 8 for Big Dataset Sampling");
                    new Exp8BigDatabase().run();
                    break;
                case "exp8_copy":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 8 for Copying Data");
                    new Exp8CopyData().run();
                    break;
                case "exp8_zip":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 8 for Zipping Data");
                    new Exp8SplitTPCH().run(args);
                    break;
                case "exp8_create":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 8 for Creating Database for TPC-H");
                    new Exp8CreateTPCH().run(args);
                    break;
                case "exp11_ada_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 11 for Ada with Adaptive Time cost");
                    new Exp11AdaTimeCost().run(args);
                    break;
                case "exp11_adaptive_result":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 11 for Adaptive Result Performance");
                    new Exp11AdaptiveResult().run(args);
                    break;
                case "exp13_base_data_spark":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 13 for Accurate Result on 24h Data");
                    new Exp13BaseDataSpark().run(args);
                    break;
                case "exp13_base_data_verdict":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 13 for Verdict Result on 24h Data");
                    new Exp13BaseDataVerdict().run(args);
                    break;
                case "exp15_check_verdict":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 15 for Checking Verdict Result");
                    new Exp15CheckVerdict().run(args);
                    break;
                case "exp16_stratified_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 16 for Stratified Increment Cost");
                    new Exp16StratifiedIncrement().run(args);
                    break;
                case "exp19_variance_increment":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 19 for Variance Increment Result");
                    new Exp19VarianceIncrement().run(args);
                    break;
                case "exp20_ada_result":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 20 for Ada Result");
                    new Exp20AdaResult().run(args);
                    break;
                case "exp20_verdict_result":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 20 for Verdict Result");
                    new Exp20VerdictResult().run(args);
                    break;
                case "exp21_time_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 21 for Time Cost");
                    new Exp21TimeCost().run(args);
                    break;
            }
        }
    }

    public static void main(String[] args) {
        new App(args);

    }
}
