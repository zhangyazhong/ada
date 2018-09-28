package daslab.exp;

/**
 * @author zyz
 * @version 2018-07-19
 */
public interface ExpRunnable {
    void run();
    default void run(String[] args) {
        for (int k = 2; k < args.length; k += 2) {
            ExpConfig.set(args[k - 1], args[k]);
        }
        run();
    }
}
