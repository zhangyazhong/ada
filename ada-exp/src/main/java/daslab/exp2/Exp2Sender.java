package daslab.exp2;

import daslab.context.ProducerContext;
import daslab.utils.AdaLogger;

/**
 * @author zyz
 * @version 2018-06-05
 */
public class Exp2Sender {
    public Exp2Sender() {
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
