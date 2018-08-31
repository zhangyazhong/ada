package daslab.server;

import daslab.context.AdaContext;
import daslab.utils.AdaLogger;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

/**
 * @author zyz
 * @version 2018-06-08
 */
public class HdfsBathReceiver {
    private AdaContext context;

    private HdfsBathReceiver(AdaContext context) {
        this.context = context;
    }

    public static HdfsBathReceiver build(AdaContext context) {
        return new HdfsBathReceiver(context);
    }

    public void receive(String location) {
        /*
        File file = new File(context.get("data.tmp.location"));
        if (file.exists()) {
            file.delete();
        }
        file.getParentFile().mkdirs();
        String command = String.format("hadoop fs -get %s %s", location, context.get("data.tmp.location"));
        call(command);
        file = new File(context.get("data.tmp.location"));
        if (file.exists() && file.isFile()) {
            AdaLogger.info(this, "New batch[" + context.increaseBatchCount() + "] arrived. HDFS location is: " + location + ". Size is " + (file.length() / 1024L / 1024L) + "MB");
            context.afterOneBatch(file.getAbsolutePath());
        } else {
            AdaLogger.info(this, "Transfer failed. HDFS location is: " + location);
        }
        */
        context.increaseBatchCount();
        context.afterOneBatch(location);
    }

    public void receive(String[] locations) {
        /*
        File file = new File(context.get("data.tmp.location"));
        file.getParentFile().mkdirs();
        File[] files = new File[locations.length];
        String[] batchLocations = new String[locations.length];
        for (int i = 0; i < locations.length; i++) {
            batchLocations[i] = context.get("data.tmp.location") + i;
            files[i] = new File(batchLocations[i]);
            if (files[i].exists()) {
                files[i].delete();
            }
            String command = String.format("hadoop fs -get %s %s", locations[i], files[i].getAbsolutePath());
            call(command);
            if (files[i].exists() && files[i].isFile()) {
                AdaLogger.info(this, "New batch[" + context.increaseBatchCount() + "] arrived. HDFS location is: " + locations[i] + ". Size is " + (files[i].length() / 1024L / 1024L) + "MB");
            } else {
                AdaLogger.info(this, "Transfer failed. HDFS location is: " + locations[i]);
            }
        }
        */
        context.increaseBatchCount();
        context.afterOneBatch(locations);
    }

    private void call(String cmd) {
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(process.getInputStream())));
            String line;
            while ((line = bufferedReader.readLine()) != null)
                AdaLogger.debug(this, "System log: " + line);
            if (process.waitFor() != 0) {
                if (process.exitValue() == 1) {
                    AdaLogger.error(this, "Call {" + cmd + "} error!");
                }
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
