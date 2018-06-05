package daslab.exp;

import daslab.exp1.Exp1;
import daslab.exp1.Exp1Cluster;
import daslab.exp1.Exp1Collect;
import daslab.exp1.Exp1Fast;
import daslab.exp2.Exp2;
import daslab.restore.RestoreModule;
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
                case "exp1":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 1");
                    new Exp1().run();
                    break;
                case "exp1_cluster":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 1 on Lab Cluster");
                    new Exp1Cluster().run();
                    break;
                case "exp1_accurate":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 1 Only Accurate");
                    new Exp1Fast().run();
                    break;
                case "exp1_collect":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 1 Collecting Results");
                    new Exp1Collect().run();
                    break;
                case "exp2":
                    AdaLogger.info(this, "Ada Exp operation: Experiment 2");
                    new Exp2().run();
                    break;
            }
        }
    }

    public static void main(String[] args) {
        new App(args);

    }
}
