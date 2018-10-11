package daslab.exp13;

import daslab.exp.ExpQueryPool;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author zyz
 * @version 2018-10-04
 */
public class Exp13BaseDataVerdictTest {

    @Test
    public void testRun() {
        List<String> QUERIES;
        /*
        QUERIES = ExpQueryPool.QUERIES_EXCEPT(
                ImmutableList.of(
                        new ExpQueryPool.WhereClause("page_count"),
                        new ExpQueryPool.WhereClause("page_size")
                ), ImmutableList.of(
                        new ExpQueryPool.GroupByClause("project_name")
                ))
                .stream().map(ExpQueryPool.QueryString::toString).collect(Collectors.toList());
        */
        QUERIES = ExpQueryPool.QUERIES_ONLY(
                new ExpQueryPool.WhereClause("page_size"),
                new ExpQueryPool.WhereClause("page_count")
        ).stream().map(ExpQueryPool.QueryString::toString).collect(Collectors.toList());
        /*
        QUERIES.addAll(ImmutableList.of(
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=3",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=4",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=5",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=6",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=7",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=8",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=9",
                "SELECT AVG(page_size) FROM wiki_ada.pagecounts WHERE page_count=10",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=3",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=4",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=5",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=6",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=7",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=8",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=9",
                "SELECT SUM(page_size) FROM wiki_ada.pagecounts WHERE page_count=10"
        ));
        */
        AtomicInteger k = new AtomicInteger();
        QUERIES.forEach(query -> {
            System.out.println(k + "|" + query);
            k.getAndIncrement();
        });
    }
}