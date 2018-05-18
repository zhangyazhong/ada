package daslab.utils;

import java.io.*;
import java.nio.channels.FileChannel;

/**
 * @author zyz
 * @version 2018-05-11
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class FileHandler {
    public static File merge(File[] files, String destPath) {
        return merge(files, destPath, true);
    }

    public static File merge(File[] files, String destPath, boolean delete) {
        File dest = new File(destPath);
        if (!dest.getParentFile().exists()) {
            dest.getParentFile().mkdirs();
        }
        if (dest.exists()) {
            dest.delete();
        }
        if (files.length == 1 && delete) {
            files[0].renameTo(dest);
        } else {
            try {
                FileChannel fileChannel = new FileOutputStream(dest, true).getChannel();
                for (File file : files) {
                    FileChannel blk = new FileInputStream(file).getChannel();
                    fileChannel.transferFrom(blk, fileChannel.size(), blk.size());
                    blk.close();
                }
                fileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (delete) {
                for (File file : files) {
                    file.delete();
                }
            }
        }
        return dest;
    }
}
