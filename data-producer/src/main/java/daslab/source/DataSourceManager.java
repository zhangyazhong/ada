package daslab.source;

import daslab.context.ProducerContext;
import daslab.utils.AdaLogger;

/**
 * @author zyz
 * @version 2018-05-10
 */
public class DataSourceManager {
    public static DataSource build(ProducerContext context) {
        String system = context.get("data.source.system").toLowerCase();
        switch (system) {
            case "hdfs":
                AdaLogger.info("[DataSourceManager] Build data source with HDFS system.");
                return HdfsDataSource.connect(context);
        }
        return null;
    }
}
