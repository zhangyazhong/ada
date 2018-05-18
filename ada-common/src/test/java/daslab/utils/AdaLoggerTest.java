package daslab.utils;

import org.junit.Test;

/**
 * @author zyz
 * @version 2018-05-09
 */
public class AdaLoggerTest {

    @Test
    public void info() {
        AdaLogger.info(this, "Logger[info] test");
    }

    @Test
    public void error() {
        AdaLogger.error(this, "Logger[error] test");
    }

    @Test
    public void debug() {
        AdaLogger.debug(this, "Logger[debug] test");
    }

    @Test
    public void warn() {
        AdaLogger.warn(this, "Logger[warn] test");
    }
}