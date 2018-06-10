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
            AdaContext context = new AdaContext();
            context.start();
            for (int day = 2; day <= 21; day++) {
                for (int hour = 0; hour < 24; hour++) {
                    String location = String.format("/home/hadoop/wiki/n_pagecounts-201601%02d-%02d0000", day, hour);
                    AdaLogger.info(this, "Send a new batch at " + location);
                    context.receive(location);
                    Thread.sleep(1000);
                }
            }
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
