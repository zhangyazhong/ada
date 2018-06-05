package daslab.exp2;

import daslab.context.AdaContext;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;

/**
 * @author zyz
 * @version 2018-06-05
 */
public class Exp2Core {
    public Exp2Core() {
    }

    public void run() {
        try {
            SystemRestore.restoreModules().forEach(RestoreModule::restore);
            AdaLogger.info(this, "Restored database.");
            Thread.sleep(1000);
            AdaContext adaContext = new AdaContext();
            adaContext.start();
            AdaLogger.info(this, "Ada Core has been started.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Exp2Core exp2Core = new Exp2Core();
        exp2Core.run();
    }
}
