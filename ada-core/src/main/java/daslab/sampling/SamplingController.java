package daslab.sampling;

import daslab.bean.AdaBatch;
import daslab.context.AdaContext;

/**
 * @author zyz
 * @version 2018-05-15
 */
public class SamplingController {
    private AdaContext context;
    private SamplingStrategy samplingStrategy;
    private SamplingStrategy resamplingStrategy;

    public SamplingController(AdaContext context) {
        this.context = context;
        switch (context.get("sampling.strategy")) {
            case "verdict":
                this.samplingStrategy = new VerdictSampling(context);
                break;
            case "incremental":
                this.samplingStrategy = new IncrementalSampling(context);
                break;
            case "reservoir":
            default:
                this.samplingStrategy = new ReservoirSampling(context);
                break;
        }
        switch (context.get("resampling.strategy")) {
            case "incremental":
                this.samplingStrategy = new IncrementalSampling(context);
                break;
            case "verdict":
            default:
                this.resamplingStrategy = new VerdictSampling(context);
                break;
        }
    }

    public SamplingStrategy getSamplingStrategy() {
        return samplingStrategy;
    }

    public SamplingStrategy getResamplingStrategy() {
        return resamplingStrategy;
    }

    public void update(AdaBatch adaBatch) {
        samplingStrategy.run(adaBatch);
    }

    public void resample(AdaBatch adaBatch) {
        resamplingStrategy.run(adaBatch);
    }
}
