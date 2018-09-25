package daslab.exp;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

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

    @Test
    public void QUERIES_EXCEPT2() {
        ExpQueryPool.QUERIES_EXCEPT(
                ImmutableList.of(
                        new ExpQueryPool.WhereClause("page_count"),
                        new ExpQueryPool.WhereClause("page_size")
                ), ImmutableList.of(
                        new ExpQueryPool.GroupByClause("project_name")
                )).forEach(System.out::println);
    }

    @Test
    public void QUERIES_EXCEPT3() {
        ExpQueryPool.QUERIES_EXCEPT(
                ImmutableList.of(
                        new ExpQueryPool.WhereClause("page_count"),
                        new ExpQueryPool.WhereClause("page_size")
                ), ImmutableList.of(
                        new ExpQueryPool.GroupByClause("project_name")
                ))
                .stream().map(ExpQueryPool.QueryString::toString).forEach(System.out::println);
    }

    @Test
    public void QUERIES_ONLY() {
        ExpQueryPool.QUERIES_ONLY(
                new ExpQueryPool.WhereClause("page_size"),
                new ExpQueryPool.WhereClause("page_count")
        ).stream().map(ExpQueryPool.QueryString::toString).forEach(System.out::println);
    }
}