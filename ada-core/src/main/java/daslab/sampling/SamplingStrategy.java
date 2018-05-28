package daslab.sampling;

import daslab.context.AdaContext;

/**
 * @author zyz
 * @version 2018-05-15
 */
public abstract class SamplingStrategy {
    private AdaContext context;

    public SamplingStrategy(AdaContext context) {
        this.context = context;
    }

    public AdaContext getContext() {
        return context;
    }

    public abstract void run();
}
