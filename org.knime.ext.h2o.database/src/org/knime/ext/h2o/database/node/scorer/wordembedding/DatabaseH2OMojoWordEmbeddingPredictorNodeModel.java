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

package org.knime.ext.h2o.database.node.scorer.wordembedding;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.ext.h2o.database.node.scorer.DatabaseH2OMojoPredictorNodeModel;
import org.knime.ext.h2o.database.node.scorer.UDFObject;
import org.knime.ext.h2o.mojo.H2OMojoPortObject;
import org.knime.ext.h2o.mojo.H2OMojoPortObjectSpec;
import org.knime.ext.h2o.mojo.nodes.scorer.H2OGeneralMojoPredictorConfig;
import org.knime.ext.h2o.mojo.nodes.scorer.H2OMojoPredictorUtils;
import org.knime.ext.h2o.mojo.nodes.scorer.wordembedding.H2OMojoWordEmbeddingPredictorConfig;
import org.knime.snowflake.h2o.companion.udf.MojoPredictor;
import org.knime.snowflake.h2o.companion.udf.MojoPredictorWordEmbedding;

/**
 * The node model of the Snowflake MOJO Predictor node which predicts a word vector.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
public final class DatabaseH2OMojoWordEmbeddingPredictorNodeModel
    extends DatabaseH2OMojoPredictorNodeModel {

    @Override
    protected void validateInternal(final DataTableSpec tableSpec, final H2OGeneralMojoPredictorConfig config)
            throws InvalidSettingsException {

        final H2OMojoWordEmbeddingPredictorConfig wordEmbeddingConfig =
                (H2OMojoWordEmbeddingPredictorConfig) config;

        final String colName = wordEmbeddingConfig.getInputColName();
        if (!tableSpec.containsName(colName)) {
            throw new InvalidSettingsException("No column with name '" + colName + "' in the input table found!");
        }
        if (wordEmbeddingConfig.isChangePredColName()
                && StringUtils.isBlank(wordEmbeddingConfig.getPredColName())) {
            throw new InvalidSettingsException("The column name for the prediction must not be empty!");
        }
    }

    @Override
    protected void validateInputTable(final DataTableSpec tableSpec) throws InvalidSettingsException {
        // do nothing
    }

    @Override
    protected UDFObject getUDFObject(final DataTableSpec inputTableSpec, final DataTableSpec resultTableSpec,
        final H2OMojoPortObject mojoPortObject, final H2OGeneralMojoPredictorConfig config)
                throws InvalidSettingsException, IOException, URISyntaxException {

        final H2OMojoWordEmbeddingPredictorConfig wordEmbeddingConfig =
                (H2OMojoWordEmbeddingPredictorConfig) config;

        return new UDFObject(inputTableSpec, resultTableSpec, mojoPortObject, getPredictorClass(),
            wordEmbeddingConfig.isConvertUnknownCategoricalLevelsToNa(),
            wordEmbeddingConfig.isEnforcePresenceOfWords(), wordEmbeddingConfig.getInputColName());
    }

    @Override
    protected H2OMojoWordEmbeddingPredictorConfig createConfig() {
        return new H2OMojoWordEmbeddingPredictorConfig();
    }

    @Override
    protected DataTableSpec getSpec(final DataTableSpec spec, final H2OMojoPortObjectSpec mojoSpec,
        final H2OGeneralMojoPredictorConfig config) {

        return H2OMojoPredictorUtils.getWordEmbeddingTableSpec(spec,
            (H2OMojoWordEmbeddingPredictorConfig) config, false);
    }

    @Override
    protected Class<? extends MojoPredictor<double[]>> getPredictorClass() {
        return MojoPredictorWordEmbedding.class;
    }


}