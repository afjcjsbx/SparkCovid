package TrendLine;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.util.Arrays;

public class Trend extends OSLTrendLine {

    protected double[] xVector(double x) { // {1, x, x*x, x*x*x, ...}
        double[] poly = new double[2];
        double xi=1;
        for(int i=0; i<=1; i++) {
            poly[i]=xi;
            xi*=x;
        }
        return poly;
    }
    @Override
    protected boolean logY() {return false;}


    @Override
    public double predict(int x) {
        double yhat = coef.preMultiply(xVector(x))[0]; // apply coefs to xVector
        if (logY()) yhat = (Math.exp(yhat)); // if we predicted ln y, we still need to get y
        return yhat;
    }

    @Override
    public void setValues(int[] y, int[] x) {
        if (x.length != y.length) {
            throw new IllegalArgumentException(String.format("The numbers of y and x values must be equal (%d != %d)",y.length,x.length));
        }
        double[][] xData = new double[x.length][];
        for (int i = 0; i < x.length; i++) {
            // the implementation determines how to produce a vector of predictors from a single x
            xData[i] = xVector(x[i]);
        }
        if(logY()) { // in some models we are predicting ln y, so we replace each y with ln y
            y = Arrays.copyOf(y, y.length); // user might not be finished with the array we were given
            for (int i = 0; i < x.length; i++) {
                y[i] = Math.log(y[i]);
            }
        }
        OLSMultipleLinearRegression ols = new OLSMultipleLinearRegression();
        ols.setNoIntercept(true); // let the implementation include a constant in xVector if desired
        ols.newSampleData(y, xData); // provide the data to the model
        coef = MatrixUtils.createColumnRealMatrix(ols.estimateRegressionParameters()); // get our coefs
    }
}