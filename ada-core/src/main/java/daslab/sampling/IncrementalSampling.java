package daslab.sampling;

import daslab.bean.Batch;
import daslab.context.AdaContext;

/**
 * @author zyz
 * @version 2018-06-05
 */
public class IncrementalSampling extends SamplingStrategy {
    public IncrementalSampling(AdaContext context) {
        super(context);
    }

    @Override
    public void run(Batch batch) {

    }

    @Override
    public String strategyName() {
        return "incremental";
    }
}
