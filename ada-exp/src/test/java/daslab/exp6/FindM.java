package daslab.exp6;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class FindM {
    private static double R = 0.75;
    private static int D = 10000;
    private static int S = (int) (D * 0.10);
    private static double[] ns = {1, 3, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500};
    private static int n = (int) (S * 1.01);

    @Test
    public void findM() throws InterruptedException {
        int before = 1000;
        for (double n1 : ns) {
            n = (int) (S * (1 + n1 / 100.0));
            int left = 1, right = before;
            for (; right <= 1000000000; right *= 2) {
                if (ratio(right).doubleValue() >= R) {
                    break;
                }
            }
            before = right;
            int m = -1;
            while (left <= right) {
                int mid = (left + right) >> 1;
                double ratio = ratio(mid).doubleValue();
                // System.out.println(String.format("[%d,%d,%d]: %f", left, right, mid, ratio));
                if (ratio >= R) {
                    m = mid;
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
            Thread.sleep(100);
            System.err.println(String.format("n: %.0f%%, m: %d", n1, m));
        }
    }

    private BigDecimal ratio(int m) {
        BigInteger up = BigInteger.ZERO;
        for (int x = Math.max(0, n - m); x <= S; x++) {
            up = up.add(C(D, x).multiply(C(m, n - x)));
        }
        BigInteger down = C(D + m, n);
        return new BigDecimal(up).divide(new BigDecimal(down), 4, BigDecimal.ROUND_HALF_EVEN);
    }

    private BigInteger C(int n, int m) {
        if (n <= m) {
            return BigInteger.valueOf(1);
        }
        BigInteger nF = F(n);
        BigInteger mF = F(m);
        BigInteger nmF = F(n - m);
        return nF.divide(mF).divide(nmF);
    }

    private BigInteger F(int n) {
        BigInteger f = new BigInteger(String.valueOf(1));
        for (int i = 2; i <= n; i++) {
            f = f.multiply(BigInteger.valueOf(i));
        }
        return f;
    }
}
