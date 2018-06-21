package daslab.exp;

import daslab.exp1.Exp1;
import daslab.exp1.Exp1Cluster;
import daslab.exp1.Exp1Collect;
import daslab.exp1.Exp1Accurate;
import daslab.exp2.Exp2Core;
import daslab.exp2.Exp2Sender;
import daslab.exp3.Exp3;
import daslab.exp3.Exp3Clean;
import daslab.exp3.Exp3Stratified;
import daslab.exp3.Exp3Uniform;
import daslab.restore.RestoreModule;
import daslab.restore.SampleRestore;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;

/**
 * Hello world!
 *
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
                    new SampleRestore().restore();
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
            }
        }
    }

    public static void main(String[] args) {
        new App(args);

    }
}
