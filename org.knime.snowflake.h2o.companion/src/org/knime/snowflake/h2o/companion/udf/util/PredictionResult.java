package org.knime.snowflake.h2o.companion.udf.util;

/**
 * Wrapper object for prediction result.
 *
 * @author Zkriya Rakhimberdiyev
 * @param <R> the result class
 */
public final class PredictionResult<R> {

	private final R m_result;

	private final double[] m_classProbabilities;

	/**
	 * Constructor for prediction result.
	 *
	 * @param result             result
	 * @param classProbabilities class probabilities
	 */
	public PredictionResult(final R result, final double[] classProbabilities) {
		this.m_result = result;
		this.m_classProbabilities = classProbabilities;
	}

	/**
	 * Gets result.
	 *
	 * @return the m_result
	 */
	public R getResult() {
		return m_result;
	}

	/**
	 * Gets class probabilities.
	 *
	 * @return the m_classProbabilities
	 */
	public double[] getClassProbabilities() {
		return m_classProbabilities;
	}
}
