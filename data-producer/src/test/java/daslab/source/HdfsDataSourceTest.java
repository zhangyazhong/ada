package daslab.source;

import daslab.context.ProducerContext;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author zyz
 * @version 2018-05-10
 */
public class HdfsDataSourceTest {

    private static ProducerContext context;
    private static DataSource dataSource;

    @BeforeClass
    public static void init() {
        context = new ProducerContext();
        dataSource = DataSourceManager.build(context);
    }

    @Test
    public void testNext() {
        dataSource.next();
    }
}
