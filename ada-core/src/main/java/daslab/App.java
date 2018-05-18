package daslab;

import daslab.context.AdaContext;

/**
 * Hello world!
 *
 */
public class App {
    public App() {
        AdaContext context = new AdaContext();
        context.start();
    }

    public static void main( String[] args ) {
        new App();
    }
}
