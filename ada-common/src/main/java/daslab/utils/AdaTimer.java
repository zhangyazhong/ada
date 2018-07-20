package daslab.utils;

public class AdaTimer {
    private static long timer;

    public static long start() {
        return timer = System.currentTimeMillis();
    }

    public static long stop() {
        return System.currentTimeMillis() - timer;
    }
}
