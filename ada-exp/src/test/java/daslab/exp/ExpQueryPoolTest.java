package daslab.exp;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author zyz
 * @version 2018-08-09
 */
public class ExpQueryPoolTest {

    @Test
    public void QUERIES() {
        ExpQueryPool.QUERIES().forEach(System.out::println);
    }

    @Test
    public void QUERIES_EXCEPT() {
        ExpQueryPool.QUERIES_EXCEPT(
                new ExpQueryPool.WhereClause("page_count"),
                new ExpQueryPool.WhereClause("page_size")
        ).forEach(System.out::println);
    }
}