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

package org.knime.ext.h2o.database.node.scorer.dimreduction;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.ext.h2o.database.node.scorer.DatabaseH2OMojoPredictorNodeModel;
import org.knime.ext.h2o.mojo.H2OMojoPortObjectSpec;
import org.knime.ext.h2o.mojo.nodes.scorer.H2OGeneralMojoPredictorConfig;
import org.knime.ext.h2o.mojo.nodes.scorer.H2OMojoPredictorUtils;
import org.knime.ext.h2o.mojo.nodes.scorer.dimreduction.H2OMojoDimReductionPredictorConfig;
import org.knime.snowflake.h2o.companion.udf.MojoPredictor;
import org.knime.snowflake.h2o.companion.udf.MojoPredictorDimReduction;

/**
 * The node model of the Snowflake MOJO predictor node which applies a dimension reduction.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
public final class DatabaseH2OMojoDimReductionPredictorNodeModel extends DatabaseH2OMojoPredictorNodeModel {

    @Override
    protected void validateInternal(final DataTableSpec tableSpec,
        final H2OGeneralMojoPredictorConfig config)
            throws InvalidSettingsException {
        // do nothing
    }

    @Override
    protected H2OMojoDimReductionPredictorConfig createConfig() {
        return new H2OMojoDimReductionPredictorConfig();
    }

    @Override
    protected DataTableSpec getSpec(final DataTableSpec spec, final H2OMojoPortObjectSpec mojoSpec,
        final H2OGeneralMojoPredictorConfig config) {
        final DataColumnSpec[] columnSpec = H2OMojoPredictorUtils
                .getDimReductionColumnSpecs(spec, mojoSpec, (H2OMojoDimReductionPredictorConfig) config);
        return new DataTableSpec(columnSpec);
    }

    @Override
    protected Class<? extends MojoPredictor<Double>> getPredictorClass() {
        return MojoPredictorDimReduction.class;
    }
}
