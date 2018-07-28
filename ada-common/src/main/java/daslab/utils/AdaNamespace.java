package daslab.utils;

import org.apache.commons.lang3.RandomStringUtils;

import java.io.Serializable;
import java.util.Map;

/**
 * @author zyz
 * @version 2018-07-27
 */
public class AdaNamespace {
    static class UniqueName implements Serializable {
        public static UniqueName first() {
            return null;
        }
    }

    private static Map<String, UniqueName> uniqueNameMap;

    public static String permenentUniqueName(String namespace) {
        return null;
    }

    public static String tempUniqueName(String namespace) {
        return String.format("%s_%s", namespace, RandomStringUtils.randomAlphanumeric(6));
    }
}
