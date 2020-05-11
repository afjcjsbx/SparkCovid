package TrendLine;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.util.Arrays;

public abstract class OSLTrendLine implements TrendLine {
    RealMatrix coef = null; // will hold prediction coefs once we get values

    protected abstract int[] xVector(int x); // create vector of values from x
    protected abstract boolean logY(); // set true to predict log of y (note: y must be positive)

    public abstract void setValues(int[] y, int[] x);
}
