package daslab.server;

import daslab.context.AdaContext;
import daslab.utils.AdaLogger;

import java.io.File;

/**
 * @author zyz
 * @version 2018-06-08
 */
public class LocalFileBatchReceiver {
    private AdaContext context;

    private LocalFileBatchReceiver(AdaContext context) {
        this.context = context;
    }

    public static LocalFileBatchReceiver build(AdaContext context) {
        return new LocalFileBatchReceiver(context);
    }

    public void receive(File file) {
        if (file.exists() && file.isFile()) {
            AdaLogger.info(this, "New batch arrived. Location is: " + file.getAbsolutePath() + ". Size is " + (file.length() / 1024L / 1024L) + "MB");
            context.afterOneBatch(file.getAbsolutePath());
        } else {
            AdaLogger.info(this, "New batch arrived. But file does not exist.");
        }
    }

    public void receive(String location) {
        File file = new File(location);
        if (file.exists() && file.isFile()) {
            AdaLogger.info(this, "New batch arrived. Location is: " + file.getAbsolutePath() + ". Size is " + (file.length() / 1024L / 1024L) + "MB");
            context.afterOneBatch(file.getAbsolutePath());
        } else {
            AdaLogger.info(this, "New batch arrived. But file does not exist.");
        }
    }
}
