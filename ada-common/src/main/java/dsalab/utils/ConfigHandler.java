package dsalab.utils;

import java.io.*;
import java.util.Map;
import java.util.Properties;

/**
 * @author zyz
 * @version 2018-05-10
 */
public class ConfigHandler {
    public static Properties load(InputStream inputStream) {
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            AdaLogger.error(ConfigHandler.class, String.format("Load properties error at %s", inputStream.toString()));
        }
        return properties;
    }

    public static Properties load(String path) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(path));
        } catch (IOException e) {
            AdaLogger.error(ConfigHandler.class, String.format("Load properties error at %s", path));
        }
        return properties;
    }

    public static Properties update(Properties properties, Map<String, String> params) {
        try {
            OutputStream fos = new FileOutputStream(properties.getProperty("self.location"));
            params.forEach(properties::setProperty);
            properties.store(fos, "");
        } catch (IOException e) {
            AdaLogger.error(ConfigHandler.class, String.format("Update properties error at %s", properties.getProperty("self.location")));
        }
        return properties;
    }

    public static Properties create(String path, Map<String, String> params) {
        Properties properties = new Properties();
        try {
            File file = new File(path);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            OutputStream fos = new FileOutputStream(path);
            properties.setProperty("self.location", path);
            params.forEach(properties::setProperty);
            properties.store(fos, "");
        } catch (IOException e) {
            AdaLogger.error(ConfigHandler.class, String.format("Create properties error at %s", path));
        }
        return properties;
    }
}
