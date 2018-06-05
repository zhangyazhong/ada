package daslab.restore;

import com.google.common.collect.ImmutableMap;
import daslab.utils.AdaLogger;
import daslab.utils.ConfigHandler;

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
                AdaLogger.info(this, "Deleted " + breakpoint.getAbsolutePath());
            }
        }
        ConfigHandler.create("/tmp/ada/source/breakpoint.conf", ImmutableMap.of(
                "self.location", "/tmp/ada/source/breakpoint.conf",
                "breakpoint.file_name", "n_pagecounts-20160108-000000",
                "breakpoint.line_number", String.valueOf(-1)
        ));
    }

    public static void main(String[] args) {
        ProducerRestore p = new ProducerRestore();
        p.restore();
    }
}
