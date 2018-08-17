package org.apache.flink.runtime.executiongraph;

public class OptimisationModelParameters {
	private static OptimisationModelParameters defaultParameters;

	public static OptimisationModelParameters defaultParameters() {
		return new OptimisationModelParameters();
	}

	/**
	 * How important the network cost is (with respect to execution speed).
	 */
	private double networkCostWeight = 0.5;

	/**
	 * How important the execution speed is (with respect to network cost).
	 */
	private double executionSpeedWeight = 0.5;

	/**
	 * How many seconds should the model take to find an optimal solution, before returning a
	 * heuristic-based one, for each task in the job.
	 */
	private double timeForEachTaskBeforeHeuristicSolution = 10;

	/**
	 * Enables/disables slot sharing optimisation
	 * */
	private boolean isSlotSharingEnabled = false;

	/**
	 * @param executionSpeedWeight                   How important the network cost is (with respect to execution speed).
	 * @param networkCostWeight                      How important the execution speed is (with respect to network cost).
	 * @param timeForEachTaskBeforeHeuristicSolution How many seconds should the model take to find an optimal solution,
	 *                                               before returning a heuristic-based one, for each task in the job.
	 * @param isSlotSharingEnabled                   Enables/disables slot sharing optimisation
	 */
	public OptimisationModelParameters(double networkCostWeight, double executionSpeedWeight, double timeForEachTaskBeforeHeuristicSolution, boolean isSlotSharingEnabled) {
		if (networkCostWeight + executionSpeedWeight != 1) {
			throw new IllegalArgumentException("networkCostWeight + executionSpeedWeight != 1");
		}
		this.networkCostWeight = networkCostWeight;
		this.executionSpeedWeight = executionSpeedWeight;
		this.timeForEachTaskBeforeHeuristicSolution = timeForEachTaskBeforeHeuristicSolution;
		this.isSlotSharingEnabled = isSlotSharingEnabled;
	}

	/**
	 * Creates with default values.
	 */
	public OptimisationModelParameters() {

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

	public boolean isSlotSharingEnabled() {
		return isSlotSharingEnabled;
	}

	public void setSlotSharingEnabled(boolean slotSharingEnabled) {
		isSlotSharingEnabled = slotSharingEnabled;
	}
}
