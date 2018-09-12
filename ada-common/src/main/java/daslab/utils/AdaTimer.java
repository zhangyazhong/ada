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

    public static String format(long time) {
        return String.format("%d:%02d.%03d", time / 60000, (time / 1000) % 60, time % 1000);
    }
}
