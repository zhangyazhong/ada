package daslab.exp8;

import daslab.utils.AdaSystem;

/**
 * @author zyz
 * @version 2018-09-21
 */
public class Exp8CopyData implements Runnable {
    @Override
    public void run() {
        String cp = "hadoop fs -cp /zyz/wiki/n_pagecounts-201601%02d-%02d0000 /zyz/wiki/n_pagecounts-201601%02d-%02d0000";
        for (int i = 1; i <= 21; i++) {
            for (int j = 0; j < 24; j++) {
                String cmd = String.format(cp, i, j, i + 21, j);
                AdaSystem.call(cmd);
            }
        }
    }
}
