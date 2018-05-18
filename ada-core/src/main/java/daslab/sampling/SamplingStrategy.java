package daslab.sampling;

import daslab.context.AdaContext;

import java.util.Set;

/**
 * @author zyz
 * @version 2018-05-15
 */
public abstract class SamplingStrategy {

    public static Set<SamplingStrategy> chooseStrategies(AdaContext context) {
        return null;
    }
    public abstract void run();
}
