package daslab.sampling;

import daslab.bean.AdaBatch;
import daslab.bean.Sample;
import daslab.context.AdaContext;
import daslab.utils.AdaLogger;

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

    public void update(Sample sample, AdaBatch adaBatch) {
        samplingStrategy.update(sample, adaBatch);
        samplingStrategy.getMetaSizes().forEach(verdictMetaSize ->
                AdaLogger.debug(this, "Final Meta Size: " + verdictMetaSize.toString()));
        samplingStrategy.getMetaNames().forEach(verdictMetaName ->
                AdaLogger.debug(this, "Final Meta Name: " + verdictMetaName.toString()));
    }

    public void resample(Sample sample, AdaBatch adaBatch, double ratio) {
        resamplingStrategy.resample(sample, adaBatch, ratio);
        resamplingStrategy.getMetaSizes().forEach(verdictMetaSize ->
                AdaLogger.debug(this, "Final Meta Size: " + verdictMetaSize.toString()));
        resamplingStrategy.getMetaNames().forEach(verdictMetaName ->
                AdaLogger.debug(this, "Final Meta Name: " + verdictMetaName.toString()));
    }
}
