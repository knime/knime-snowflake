/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 */

package org.knime.snowflake.h2o.companion.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.knime.snowflake.h2o.companion.udf.util.PredictionResult;

import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.AutoEncoderModelPrediction;

/**
 * {@link MojoPredictor} implementation for auto encoding.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class MojoPredictorAutoEncoder extends AbstractMojoPreditor<Double> {

	@Override
	public PredictionResult<Double> predictInternal(final RowData row) throws PredictException {
		final AutoEncoderModelPrediction prediction = (AutoEncoderModelPrediction) getPredictor().predict(row);
		final RowData reconstructedRowData = prediction.reconstructedRowData;
		final double[] result = new double[getPredictionsSize()];

		final List<String[]> domainValuesCollection = new ArrayList<>();

		for (final String colName : getNames()) {
			final Object r = reconstructedRowData.get(colName);
			if (r instanceof Double) {
				domainValuesCollection.add(null);
			}
			// categorical fields will be represented as a map of the domain values to the reconstructed values
			else if (r instanceof Map) {
				domainValuesCollection.add(getDomainValues(colName));
			} else {
				throw new PredictException("Unexpected data type of reconstructed value: " + r.getClass().getName());
			}
		}

		int i = 0;
		int j = 0;
		for (final String colName : getNames()) {
			final Object r = reconstructedRowData.get(colName);
			final String[] domainValues = domainValuesCollection.get(j++);
			if (domainValues == null) {
				result[i++] = (double) r;
			}
			// categorical fields will be represented as a map of the domain values to the reconstructed values
			else {
				@SuppressWarnings("unchecked") // its given by implementation that this cast will always work
				final Map<String, Double> map = (Map<String, Double>) r;
				for (final String domainValue : domainValues) {
					final Object r2 = map.get(domainValue);
					result[i++] = (double) r2;
				}
				// probability for a missing value is also contained in the map
				final Object r2 = map.get(null);
				result[i++] = (double) r2;
			}
		}

		if (result.length > 1) {
			return new PredictionResult<>(result[0], Arrays.copyOfRange(result, 1, result.length));
		}
		return new PredictionResult<>(result[0], null);
	}
}
