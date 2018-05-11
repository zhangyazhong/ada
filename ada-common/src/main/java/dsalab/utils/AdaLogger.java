package dsalab.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author zyz
 * @version 2018-05-09
 */
public class AdaLogger {
    private final static Logger logger = LogManager.getLogger(AdaLogger.class);

    public static void info(Object msg) {
        logger.info(msg);
    }

    public static void debug(Object msg) {
        logger.debug(msg);
    }

    public static void error(Object msg) {
        logger.error(msg);
    }

    public static void warn(Object msg) {
        logger.warn(msg);
    }

    public static void info(Object caller, String msg) {
        info(String.format("[%s] %s", caller.getClass().getSimpleName(), msg));
    }

    public static void error(Object caller, String msg) {
        error(String.format("[%s] %s", caller.getClass().getSimpleName(), msg));
    }

    public static void debug(Object caller, String msg) {
        debug(String.format("[%s] %s", caller.getClass().getSimpleName(), msg));
    }

    public static void warn(Object caller, String msg) {
        warn(String.format("[%s] %s", caller.getClass().getSimpleName(), msg));
    }

    public static void info(Object caller, String msg, String forEveryLine) {
        String tokens[] = msg.split("\n");
        for (String token : tokens) {
            info(caller, forEveryLine + token);
        }
    }

    public static void debug(Object caller, String msg, String forEveryLine) {
        String tokens[] = msg.split("\n");
        for (String token : tokens) {
            debug(caller, forEveryLine + token);
        }
    }
}
