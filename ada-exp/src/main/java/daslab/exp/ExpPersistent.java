package daslab.exp;

import java.io.File;

/**
 * @author zyz
 * @version 2018-08-07
 */
public interface ExpPersistent {
    String outputPath();
    default File outputFile() {
        return new File(outputPath());
    }
}
