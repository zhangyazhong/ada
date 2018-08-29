package daslab.exp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zyz
 * @version 2018-08-09
 */
public class ExpQueryPool {
    public static class QueryString implements Comparable<QueryString> {
        private String query;
        private String aggregationType;

        public QueryString(String query) {
            this.query = query;
            this.aggregationType = StringUtils.substringBefore(StringUtils.substringBetween(query, "SELECT", "FROM"), "(").trim();
        }

        public String getAggregationType() {
            return aggregationType;
        }

        public List<String> getWhereColumn() {
            List<String> columns = Lists.newLinkedList();
            String[] whereClauses = StringUtils.substringAfter(query, "WHERE").length() > 0 ?
                    StringUtils.substringAfter(query, "WHERE").split("AND") : new String[0];
            for (String whereClause : whereClauses) {
                columns.add(whereClause.split("[<>=]")[0].trim());
            }
            return columns;
        }

        public boolean containsWhereColumn(WhereClause whereClause) {
            return getWhereColumn().stream().anyMatch(whereColumn -> whereColumn.equals(whereClause.getColumn()));
        }

        @Override
        public int compareTo(@NotNull QueryString another) {
            if (!aggregationType.equals(another.aggregationType)) {
                return aggregationType.compareTo(another.aggregationType);
            }
            return query.compareTo(another.query);
        }

        @Override
        public String toString() {
            return query;
        }
    }

    public static class WhereClause {
        private String column;

        public WhereClause(String column) {
            this.column = column;
        }

        public String getColumn() {
            return this.column;
        }
    }

    private final static List<String> SELECTS = ImmutableList.of("page_count");
    private final static List<String> AGGREGATION_FUNCTIONS = ImmutableList.of("AVG", "SUM", "COUNT");
    private final static List<String> PROJECT_NAMES = ImmutableList.of("aa", "uk", "www", "kk", "zh.d", "th.mw", "en.mw");
    private final static List<String> PAGE_COUNTS = ImmutableList.of("1", "2", "3", "4", "15");
    private final static List<String> PAGE_SIZES = ImmutableList.of("6000", "7000", "8000", "9000", "10000", "12000", "16000", "23000", "36000", "80000");
    private static List<QueryString> QUERIES;

    static {
        QUERIES = Lists.newLinkedList();
        QUERIES.addAll(generateNoCase());
        QUERIES.addAll(generatePageSizeCase());
        QUERIES.addAll(generatePageCountCase());
        QUERIES.addAll(generateProjectNameCase());
        QUERIES.addAll(generatePageSizeWithPageCountCase());
        QUERIES.addAll(generateGroupByCase());
        QUERIES.addAll(generateGroupByWithPageCountCase());
        QUERIES.sort(QueryString::compareTo);
    }

    public static List<QueryString> QUERIES() {
        return QUERIES;
    }

    public static List<QueryString> QUERIES_EXCEPT(WhereClause... exceptWheres) {
        List<WhereClause> _exceptWheres = Lists.newArrayList(exceptWheres);
        return QUERIES().stream()
                .filter(queryString -> _exceptWheres.stream().noneMatch(queryString::containsWhereColumn))
                .collect(Collectors.toList());
    }

    private static List<QueryString> generateNoCase() {
        List<QueryString> QUERIES = Lists.newLinkedList();
        String QUERY_FORMAT = "SELECT %s(%s) FROM %s";
        AGGREGATION_FUNCTIONS.forEach(aggregation
                -> SELECTS.forEach(select
                -> QUERIES.add(new QueryString(String.format(QUERY_FORMAT, aggregation, select, ExpConfig.tableInSQL())))));
        return QUERIES;
    }

    private static List<QueryString> generatePageSizeCase() {
        List<QueryString> QUERIES = Lists.newLinkedList();
        final int[] chosen = {1, 5, 9};
        final String[] symbols = {"<", ">", ">="};
        String QUERY_FORMAT = "SELECT %s(%s) FROM %s WHERE page_size%s%s";
        AGGREGATION_FUNCTIONS.forEach(aggregation
                -> SELECTS.forEach(select
                -> {
                    for (int k = 0; k < chosen.length; k++) {
                        QUERIES.add(new QueryString(String.format(QUERY_FORMAT, aggregation, select, ExpConfig.tableInSQL(), symbols[k], PAGE_SIZES.get(chosen[k]))));
                    }
                }));
        return QUERIES;
    }

    private static List<QueryString> generatePageCountCase() {
        List<QueryString> QUERIES = Lists.newLinkedList();
        final int[] chosen = {0, 3, 4};
        final String[] symbols = {">", "<", ">="};
        String QUERY_FORMAT = "SELECT %s(%s) FROM %s WHERE page_count%s%s";
        AGGREGATION_FUNCTIONS.forEach(aggregation
                -> SELECTS.forEach(select
                -> {
                    for (int k = 0; k < chosen.length; k++) {
                        QUERIES.add(new QueryString(String.format(QUERY_FORMAT, aggregation, select, ExpConfig.tableInSQL(), symbols[k], PAGE_COUNTS.get(chosen[k]))));
                    }
                }));
        return QUERIES;
    }

    private static List<QueryString> generateProjectNameCase() {
        List<QueryString> QUERIES = Lists.newLinkedList();
        String QUERY_FORMAT = "SELECT %s(%s) FROM %s WHERE project_name='%s'";
        AGGREGATION_FUNCTIONS.forEach(aggregation
                -> SELECTS.forEach(select
                -> PROJECT_NAMES.forEach(projectName
                -> QUERIES.add(new QueryString(String.format(QUERY_FORMAT, aggregation, select, ExpConfig.tableInSQL(), projectName))))));
        return QUERIES;
    }

    private static List<QueryString> generatePageSizeWithPageCountCase() {
        List<QueryString> QUERIES = Lists.newLinkedList();
        String QUERY_FORMAT = "SELECT %s(%s) FROM %s WHERE page_size%s%s AND page_count%s%s";
        List<String[]> CASES = ImmutableList.of(
                new String[]{">", "2", ">=", "2"},
                new String[]{">", "5", "<", "3"},
                new String[]{"<", "6", "<=", "1"}
        );
        AGGREGATION_FUNCTIONS.forEach(aggregation
                -> SELECTS.forEach(select
                -> CASES.forEach(_case
                -> QUERIES.add(new QueryString(String.format(QUERY_FORMAT, aggregation, select, ExpConfig.tableInSQL(),
                        _case[0], PAGE_SIZES.get(Integer.parseInt(_case[1])),
                        _case[2], PAGE_COUNTS.get(Integer.parseInt(_case[3]))))))));
        return QUERIES;
    }

    private static List<QueryString> generateGroupByCase() {
        List<QueryString> QUERIES = Lists.newLinkedList();
        List<String> QUERY_FORMATs = ImmutableList.of(
                "SELECT project_name,%s(page_count) FROM %s GROUP BY project_name",
                "SELECT project_name,%s(page_size) FROM %s GROUP BY project_name"
        );
        AGGREGATION_FUNCTIONS.forEach(aggregation
                -> QUERY_FORMATs.forEach((QUERY_FORMAT)
                -> QUERIES.add(new QueryString(String.format(QUERY_FORMAT, aggregation, ExpConfig.tableInSQL())))));
        return QUERIES;
    }

    private static List<QueryString> generateGroupByWithPageCountCase() {
        List<QueryString> QUERIES = Lists.newLinkedList();
        String QUERY_FORMAT = "SELECT project_name, %s(page_count) FROM %s WHERE page_count>1 GROUP BY project_name";
        AGGREGATION_FUNCTIONS.forEach(aggregation
                -> QUERIES.add(new QueryString(String.format(QUERY_FORMAT, aggregation, ExpConfig.tableInSQL()))));
        return QUERIES;
    }
}
