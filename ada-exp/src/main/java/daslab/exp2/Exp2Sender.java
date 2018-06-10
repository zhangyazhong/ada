package daslab.exp2;

import daslab.context.AdaContext;
import daslab.context.ProducerContext;
import daslab.utils.AdaLogger;

/**
 * @author zyz
 * @version 2018-06-05
 */
public class Exp2Sender {
    private AdaContext context;

    public Exp2Sender() {
        context = new AdaContext();
    }

    public void run() {
        ProducerContext producerContext = new ProducerContext();
        producerContext.start();
        AdaLogger.info(this, "Data Producer has been started.");
    }

    public static void main(String[] args) {
        Exp2Sender exp2Sender = new Exp2Sender();
        exp2Sender.run();
    }
}
