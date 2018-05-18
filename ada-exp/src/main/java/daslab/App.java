package daslab;

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
            switch (args[0]) {
                case "restore":
                    AdaLogger.info(this, "Ada Exp operation: RESTORE");
                    SystemRestore.restoreModules().forEach(RestoreModule::restore);
                    break;
            }
        }
    }

    public static void main(String[] args) {
        new App(args);

    }
}
