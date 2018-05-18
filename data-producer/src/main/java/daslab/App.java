package daslab;

import daslab.context.ProducerContext;

/**
 * Hello world!
 *
 */
public class App {
    public App() {
        ProducerContext context = new ProducerContext();
        context.start();
    }

    public static void main( String[] args ) {
        new App();
    }
}
