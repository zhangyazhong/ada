package daslab.sampling;

import daslab.bean.Batch;
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
    public void run(Batch batch) {

    }

    @Override
    public String name() {
        return "random uniform";
    }
}
