package org.apache.flink.runtime.executiongraph;

public class OptimisationModelParameters {
	private static OptimisationModelParameters defaultParameters;

	public static OptimisationModelParameters defaultParameters() {
		if(defaultParameters == null) {
			defaultParameters = new OptimisationModelParameters();
		}
		return defaultParameters;
	}

	/**
	 * How important the network cost is (with respect to execution speed).
	 * */
	private double networkCostWeight = 0.5;

	/**
	 * How important the execution speed is (with respect to network cost).
	 * */
	private double executionSpeedWeight = 0.5;

	/**
	 * How many seconds should the model take to find an optimal solution, before returning a
	 * heuristic-based one, for each task in the job.
	 * */
	private double timeForEachTaskBeforeHeuristicSolution = 10;

	/**
	 * @param executionSpeedWeight How important the network cost is (with respect to execution speed).
	 * @param networkCostWeight How important the execution speed is (with respect to network cost).
	 * @param timeForEachTaskBeforeHeuristicSolution How many seconds should the model take to find an optimal solution,
	 *                                               before returning a heuristic-based one, for each task in the job.
	 * */
	public OptimisationModelParameters(double networkCostWeight, double executionSpeedWeight, double timeForEachTaskBeforeHeuristicSolution) {
		if(networkCostWeight + executionSpeedWeight != 1) {
			throw new IllegalArgumentException("networkCostWeight + executionSpeedWeight != 1");
		}
		this.networkCostWeight = networkCostWeight;
		this.executionSpeedWeight = executionSpeedWeight;
		this.timeForEachTaskBeforeHeuristicSolution = timeForEachTaskBeforeHeuristicSolution;
	}

	/**
	 * Creates with default values.
	 * */
	public OptimisationModelParameters() {

	}

	public static OptimisationModelParameters getDefaultParameters() {
		return defaultParameters;
	}

	public static void setDefaultParameters(OptimisationModelParameters defaultParameters) {
		OptimisationModelParameters.defaultParameters = defaultParameters;
	}

	public double getNetworkCostWeight() {
		return networkCostWeight;
	}

	public void setNetworkCostWeight(double networkCostWeight) {
		this.networkCostWeight = networkCostWeight;
		this.executionSpeedWeight = 1 - networkCostWeight;
	}

	public double getExecutionSpeedWeight() {
		return executionSpeedWeight;
	}

	public void setExecutionSpeedWeight(double executionSpeedWeight) {
		this.executionSpeedWeight = executionSpeedWeight;
		this.networkCostWeight = 1 - executionSpeedWeight;
	}

	public double getTimeForEachTaskBeforeHeuristicSolution() {
		return timeForEachTaskBeforeHeuristicSolution;
	}

	public void setTimeForEachTaskBeforeHeuristicSolution(double timeForEachTaskBeforeHeuristicSolution) {
		this.timeForEachTaskBeforeHeuristicSolution = timeForEachTaskBeforeHeuristicSolution;
	}
}
