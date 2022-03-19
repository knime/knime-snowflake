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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.knime.snowflake.h2o.companion.udf.util.PredictionResult;

import hex.ModelCategory;
import hex.genmodel.MojoModel;
import hex.genmodel.MojoReaderBackend;
import hex.genmodel.MojoReaderBackendFactory;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.EasyPredictModelWrapper.Config;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;

abstract class AbstractMojoPreditor<R> implements MojoPredictor<R> {

	private EasyPredictModelWrapper m_predictor;
	private MojoModel m_mojoModel;
	private String[] m_inputColumnNames;
	private File m_mojoModelFile;

	protected void validateColumnNames(final MojoModel mojoModel, final String... inputColumnNames) {
		final Set<String> mojoColumnNames = new HashSet<>(Arrays.asList(mojoModel.getNames()));

		for (final String inputColumnName : inputColumnNames) {
			if (!mojoColumnNames.contains(inputColumnName)) {
				throw new IllegalArgumentException("MOJO does not contain table column name " + inputColumnName);
			}
		}
	}

	@Override
	public synchronized void init(final File mojoModelFile, final boolean convertUnknownCategoricalLevelsToNa,
			final String... inputColumnNames) throws IOException {
		// needs to be synchronized to ensure proper initialization during parallel
		// execution
		if (m_mojoModelFile == null || !m_mojoModelFile.equals(mojoModelFile)) {
			m_mojoModelFile = mojoModelFile;
			final MojoReaderBackend backend = MojoReaderBackendFactory.createReaderBackend(m_mojoModelFile);
			final MojoModel mojoModel = MojoModel.load(backend);
			final Config config = new Config();
			config.setModel(mojoModel);
			config.setConvertUnknownCategoricalLevelsToNa(convertUnknownCategoricalLevelsToNa);
			m_predictor = new EasyPredictModelWrapper(config);
			m_mojoModel = mojoModel;
			validateColumnNames(mojoModel, inputColumnNames);
			m_inputColumnNames = inputColumnNames;
		}
	}

	public AbstractMojoPreditor() {
	}

	@Override
	public PredictionResult<R> predict(final Object... inputData) throws PredictException {
		final RowData row = parseRowData(inputData);
		return predictInternal(row);
	}

	private RowData parseRowData(final Object... inputData) throws PredictException {
		if (inputData.length != m_inputColumnNames.length) {
			throw new PredictException(String.format("Input data size %d different form model input size %d",
					inputData.length, m_inputColumnNames.length));
		}
		final RowData result = new RowData();
		for (int i = 0, length = inputData.length; i < length; i++) {
			if (inputData[i] != null) {
				if (inputData[i] instanceof Number) {
					final Number numb = (Number) inputData[i];
					result.put(getInputColumnNames()[i], numb.doubleValue());
				} else {
					// String type
					result.put(getInputColumnNames()[i], inputData[i]);
				}
			}
		}
		return result;
	}

	protected abstract PredictionResult<R> predictInternal(RowData row) throws PredictException;

	/**
	 * @return the predictor
	 */
	protected EasyPredictModelWrapper getPredictor() {
		return m_predictor;
	}

	/**
	 * @return the modelCategory
	 */
	protected ModelCategory getModelCategory() {
		return m_mojoModel.getModelCategory();
	}

	/**
	 * The names of all columns used, including response and offset columns.
	 *
	 * @return names of all columns used
	 */
	protected String[] getNames() {
		return m_mojoModel.getNames();
	}

	/**
	 * Returns domain values for all columns, including the response column.
	 *
	 * @return domain values for all columns
	 */
	protected String[] getDomainValues(final String columnName) {
		return m_mojoModel.getDomainValues(columnName);
	}

	/**
	 * @return the inputColumnNames
	 */
	protected String[] getInputColumnNames() {
		return m_inputColumnNames;
	}

	/**
	 * Returns the expected size of the prediction. If no expectation can be given, -1 is returned.
	 *
	 * @return the expected prediction size
	 */
	protected int getPredictionsSize() {
		int result;
		try {
			result = m_mojoModel.getPredsSize(m_mojoModel.getModelCategory());
		} catch (final UnsupportedOperationException e) {
			result = -1;
		}
		return result;
	}
}