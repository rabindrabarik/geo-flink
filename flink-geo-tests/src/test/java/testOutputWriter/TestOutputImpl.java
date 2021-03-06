package testOutputWriter;

import org.apache.flink.api.java.tuple.Tuple6;

public class TestOutputImpl extends Tuple6<Double, Double, String, String, String, Long> implements TestOutput {
	public TestOutputImpl(Double networkCost, Double executionSpeed, String schedulerClassName, String jobGraphClassName, String instanceSetClassName, Long elapsedTime) {
		this.f0 = networkCost;
		this.f1 = executionSpeed;
		this.f2 = schedulerClassName;
		this.f3 = jobGraphClassName;
		this.f4 = instanceSetClassName;
		this.f5 = elapsedTime;
	}

	public Object[] getFields() {
		return new Object[]{f0, f1, f2, f3, f4, f5};
	}

	public String[] getFieldNames() {
		return new String[]{"networkCost", "executionSpeed", "schedulerClassName", "jobGraphCalssName", "instanceSetClassName", "modelSolveTime [ms]"};
	}
}
