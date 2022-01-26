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
import java.util.ArrayList;
import java.util.List;

import hex.ModelCategory;
import hex.genmodel.MojoModel;
import hex.genmodel.MojoReaderBackend;
import hex.genmodel.MojoReaderBackendFactory;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.EasyPredictModelWrapper.Config;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;

abstract class AbstractMojoPreditor<R extends Object> implements MojoPredictor<R> {

	private EasyPredictModelWrapper m_predictor;
	private ModelCategory m_modelCategory;
	private double[] m_classProbabilities;
	private R m_result;
	private String[] m_inputColumnNames;
	private File m_mojoModelFile;

	private static String[] getInputColumnNames(final MojoModel mojoModel) {
		final String[] colNames = mojoModel.getNames();
		final String responseName = mojoModel.getResponseName();
		final List<String> inputColNames = new ArrayList<>(colNames.length);
		for (final String colName : colNames) {
			if (!colName.equals(responseName)) {
				inputColNames.add(colName);
			}
		}
		return inputColNames.toArray(new String[0]);
	}

	@Override
	public synchronized void init(final File mojoModelFile) throws IOException {
		// needs to be synchronized to ensure proper initialization during parallel
		// execution
		if (m_mojoModelFile == null || !m_mojoModelFile.equals(mojoModelFile)) {
			m_mojoModelFile = mojoModelFile;
			final MojoReaderBackend backend = MojoReaderBackendFactory.createReaderBackend(m_mojoModelFile);
			final MojoModel mojoModel = MojoModel.load(backend);
			final Config config = new Config();
			config.setModel(mojoModel);
			config.setConvertUnknownCategoricalLevelsToNa(true);
			m_predictor = new EasyPredictModelWrapper(config);
			m_modelCategory = mojoModel.getModelCategory();
			m_inputColumnNames = getInputColumnNames(mojoModel);
		}
	}

	public AbstractMojoPreditor() {
	}

	@SuppressWarnings("boxing")
	@Override
	public void predict(final Object... inputData) throws PredictException {
		if (inputData.length != m_inputColumnNames.length) {
			throw new PredictException(String.format("Input data size %d different form model input size %d",
					inputData.length, m_inputColumnNames.length));
		}
		final RowData row = new RowData();
		for (int i = 0, length = inputData.length; i < length; i++) {
			row.put(getInputColumnNames()[i], inputData[i]);
		}
		m_result = predictInternal(row);
	}

	protected abstract R predictInternal(RowData row) throws PredictException;

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
		return m_modelCategory;
	}

	/**
	 * @return the inputColumnNames
	 */
	protected String[] getInputColumnNames() {
		return m_inputColumnNames;
	}

	@Override
	public R getResult() {
		return m_result;
	}

	/**
	 * @param classProbabilities the classProbabilities to set
	 */
	protected void setClassProbabilities(final double[] classProbabilities) {
		m_classProbabilities = classProbabilities;
	}

	@Override
	public double[] getClassProbabilities() {
		return m_classProbabilities;
	}

}