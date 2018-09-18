package daslab.exp;

import daslab.exp1.Exp1;
import daslab.exp1.Exp1Cluster;
import daslab.exp1.Exp1Collect;
import daslab.exp1.Exp1Accurate;
import daslab.exp2.Exp2Core;
import daslab.exp2.Exp2Sender;
import daslab.exp3.*;
import daslab.exp4.*;
import daslab.exp5.*;
import daslab.exp6.Exp6Variance;
import daslab.exp7.Exp7AdaTimeCost;
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
                    new Exp5AdaTimeCost().run();
                    break;
                case "exp5_verdict_cost":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 5 for Verdict Time Cost");
                    new Exp5VerdictTimeCost().run();
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
            }
        }
    }

    public static void main(String[] args) {
        new App(args);

    }
}
