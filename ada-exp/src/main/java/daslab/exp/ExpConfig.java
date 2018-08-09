package daslab.exp;

import com.google.common.collect.Maps;
import daslab.utils.ConfigHandler;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class ExpConfig {
    public final static String[] CONFIGS = {"classpath: env.properties", "/tmp/ada/config/ada_exp.properties"};

    public static int HOUR_START = 24 * 1 - 1;
    public static int HOUR_TOTAL = 24 * 2;
    public static int HOUR_INTERVAL = 1;

    public final static Map<String, String> ENV = Maps.newHashMap();

    public static String get(String key) {
        return ENV.get(key);
    }

    static {
        for (String config : CONFIGS) {
            try {
                InputStream inputStream;
                if (config.startsWith("classpath")) {
                    inputStream = ExpConfig.class.getClassLoader().getResourceAsStream(StringUtils.substringAfter(config, "classpath:").trim());
                } else {
                    File file = new File(config);
                    if (!file.exists()) {
                        continue;
                    }
                    inputStream = new FileInputStream(config);
                }
                Properties properties = ConfigHandler.load(inputStream);
                for (String prop : properties.stringPropertyNames()) {
                    String value = properties.getProperty(prop).trim();
                    ENV.put(prop, value);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        HOUR_START = StringUtils.isNumeric(get("exp.hour.start")) ? Integer.parseInt(get("exp.hour.start")) : HOUR_START;
        HOUR_TOTAL = StringUtils.isNumeric(get("exp.hour.total")) ? Integer.parseInt(get("exp.hour.total")) : HOUR_TOTAL;
        HOUR_INTERVAL = StringUtils.isNumeric(get("exp.hour.interval")) ? Integer.parseInt(get("exp.hour.interval")) : HOUR_INTERVAL;
    }

    public static String tableInSQL() {
        return String.format("%s.%s", get("data.table.schema"), get("data.table.name"));
    }
}
