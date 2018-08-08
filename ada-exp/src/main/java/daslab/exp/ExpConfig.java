package daslab.exp;

import com.google.common.collect.Maps;
import daslab.utils.ConfigHandler;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class ExpConfig {
    public final static String[] CONFIGS = {"env.properties", "/tmp/ada/config/ada_exp.properties"};

    public final static int[] UNIFORM_SAMPLE_RATIO = {10};
    public final static int[] STRATIFIED_SAMPLE_RATIO = {10};
    public final static String[] STRATIFIED_SAMPLE_COLUMN = {"project_name"};

    public static int HOUR_START = 24 * 1 - 1;
    public static int HOUR_TOTAL = 24 * 2;

    public final static Map<String, String> ENV = Maps.newHashMap();

    public static String get(String key) {
        return ENV.get(key);
    }

    static {
        for (String config : CONFIGS) {
            File file = new File(config);
            if (!file.exists()) {
                continue;
            }
            InputStream inputStream = ExpConfig.class.getClassLoader().getResourceAsStream(config);
            Properties properties = ConfigHandler.load(inputStream);
            for (String prop : properties.stringPropertyNames()) {
                String value = properties.getProperty(prop).trim();
                ENV.put(prop, value);
            }
        }
        HOUR_START = StringUtils.isNumeric(get("exp.hour.start")) ? Integer.parseInt(get("exp.hour.start")) : HOUR_START;
        HOUR_TOTAL = StringUtils.isNumeric(get("exp.hour.total")) ? Integer.parseInt(get("exp.hour.total")) : HOUR_TOTAL;
    }
}
