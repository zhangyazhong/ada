package daslab.sampling;

import daslab.bean.AdaBatch;
import daslab.context.AdaContext;

/**
 * @author zyz
 * @version 2018-05-15
 */
public class RandomUniformSampling extends SamplingStrategy {
    public RandomUniformSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(AdaBatch adaBatch) {

    }

    @Override
    public String name() {
        return "random uniform";
    }
}
