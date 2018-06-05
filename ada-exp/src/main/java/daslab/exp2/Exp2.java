package daslab.exp2;

import daslab.context.AdaContext;
import daslab.context.ProducerContext;
import daslab.restore.RestoreModule;
import daslab.restore.SystemRestore;
import daslab.utils.AdaLogger;

/**
 * @author zyz
 * @version 2018-06-05
 */
public class Exp2 {
    public Exp2() {
    }

    public void run() {
        SystemRestore.restoreModules().forEach(RestoreModule::restore);
        AdaLogger.info(this, "Restored database.");
        try {
            Thread.sleep(1000);
            AdaContext adaContext = new AdaContext();
            adaContext.start();
            AdaLogger.info(this, "Ada Core has been started.");
            Thread.sleep(1000);
            ProducerContext producerContext = new ProducerContext();
            producerContext.start();
            AdaLogger.info(this, "Data Producer has been started.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Exp2 exp2 = new Exp2();
        exp2.run();
    }
}
