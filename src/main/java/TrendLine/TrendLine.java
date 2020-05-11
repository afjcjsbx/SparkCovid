package TrendLine;

public interface TrendLine {
    public void setValues(int[] y, int[] x); // y ~ f(x)
    public double predict(int x); // get a predicted y for a given x
}
