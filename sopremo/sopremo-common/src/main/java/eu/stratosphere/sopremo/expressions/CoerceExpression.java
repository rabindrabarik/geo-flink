package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.type.JsonNode;

@OptimizerHints(scope = Scope.NUMBER)
public class CoerceExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1954495592440005318L;

	private final Class<? extends JsonNode> targetType;

	private EvaluationExpression valueExpression;

	public CoerceExpression(final Class<? extends JsonNode> targetType, final EvaluationExpression value) {
		this.targetType = targetType;
		this.valueExpression = value;
	}

	public CoerceExpression(final Class<? extends JsonNode> targetType) {
		this(targetType, EvaluationExpression.VALUE);
	}

	public EvaluationExpression getValueExpression() {
		return this.valueExpression;
	}

	public void setValueExpression(EvaluationExpression valueExpression) {
		if (valueExpression == null)
			throw new NullPointerException("valueExpression must not be null");

		this.valueExpression = valueExpression;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		return TypeCoercer.INSTANCE.coerce(this.valueExpression.evaluate(node, context), this.targetType);
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append('(').append(this.targetType).append(')');
		if(valueExpression != EvaluationExpression.VALUE)
			builder.append(' ').append(valueExpression);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.targetType.hashCode();
		result = prime * result + this.valueExpression.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;

		final CoerceExpression other = (CoerceExpression) obj;
		return this.targetType == other.targetType && this.valueExpression.equals(other.valueExpression);
	}

}
