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
}