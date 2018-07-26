package daslab.utils;

import java.util.Map;

/**
 * @author zyz
 * @version 2018-07-26
 */
public class AdaFormatter {

    public static String formatString(String origin, Map<String, Object> params) {
        String target = origin;
        for (Map.Entry entry : params.entrySet()) {
            target = target.replaceAll(String.format("%%%s%%", entry.getKey()), entry.getValue().toString());
        }
        return target;
    }
}
