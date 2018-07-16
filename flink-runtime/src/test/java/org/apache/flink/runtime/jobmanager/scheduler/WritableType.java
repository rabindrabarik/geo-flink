package org.apache.flink.runtime.jobmanager.scheduler;

public abstract class WritableType {

	protected Object[] params;


	public String getClassNameString() {
		StringBuilder out = new StringBuilder(this.getClass().getSimpleName());

		if(params == null) {
			return out.toString();
		}

		out.append("(");

		for (int i = 0; i < params.length; i++) {
			out.append(params[i]);
			if(i < params.length -1) {
				out.append(",");
			}
		}

		out.append(")");

		return out.toString();
	}
}
