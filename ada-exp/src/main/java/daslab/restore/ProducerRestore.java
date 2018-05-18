package daslab.restore;

import daslab.utils.AdaLogger;

import java.io.File;

public class ProducerRestore implements RestoreModule {
    public ProducerRestore() {
        restore();
    }

    public void restore() {
        restoreBreakpoint();
    }

    private void restoreBreakpoint() {
        File breakpoint = new File("/tmp/ada/source/breakpoint.conf");
        if (breakpoint.exists()) {
            if (breakpoint.delete()) {
                AdaLogger.info("Deleted " + breakpoint.getAbsolutePath());
            }
        }
    }

    public static void main(String[] args) {
        ProducerRestore p = new ProducerRestore();
        p.restore();
    }
}
