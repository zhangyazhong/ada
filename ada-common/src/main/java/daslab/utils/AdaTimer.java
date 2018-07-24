package daslab.utils;

public class AdaTimer {
    private long timer;

    public static AdaTimer create() {
        AdaTimer timer = new AdaTimer();
        timer.start();
        return timer;
    }

    public long start() {
        return timer = System.currentTimeMillis();
    }

    public long stop() {
        return System.currentTimeMillis() - timer;
    }
}
