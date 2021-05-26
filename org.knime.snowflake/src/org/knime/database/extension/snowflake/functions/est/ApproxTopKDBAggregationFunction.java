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

package org.knime.database.extension.snowflake.functions.est;

import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.function.aggregation.DBAggregationFunction;
import org.knime.database.function.aggregation.DBAggregationFunctionFactory;
import org.knime.database.function.aggregation.impl.functions.parameter.AbstractNumberDBAggregationFunction;

/**
 * Top K DB aggregation function.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class ApproxTopKDBAggregationFunction extends AbstractNumberDBAggregationFunction {

    private static final String LABEL = "APPROX_TOP_K_ACCUMULATE";

    private static final int MAX_COUNTERS = 100000;

    private AdditionalNumberParameterSettingsPanel m_countersPrameterPanel;

    private final AdditionalNumberParameterSettings m_countersParameterSettings;

    private final String m_countersParameterLabel;

    /** Factory for the parent class. */
    public static final class Factory implements DBAggregationFunctionFactory {

        @Override
        public String getId() {
            return LABEL;
        }

        @Override
        public DBAggregationFunction createInstance() {
            return new ApproxTopKDBAggregationFunction();
        }
    }

    /**
     * Constructor.
     */
    private ApproxTopKDBAggregationFunction() {
        super("k", 1, Integer.MAX_VALUE, 1);
        m_countersParameterLabel = "Counters:";
        m_countersParameterSettings = new AdditionalNumberParameterSettings("counters", 1, MAX_COUNTERS, 1);
    }

    @Override
    public String getId() {
        return LABEL;
    }

    @Override
    public String getLabel() {
        return LABEL;
    }

    @Override
    public boolean isCompatible(final DataType type) {
        return true;
    }

    @Override
    public DataType getType(final DataType originalType) {
        return StringCell.TYPE;
    }

    @Override
    public String getDescription() {
        return "Returns the Space-Saving summary at the end of aggregation.";
    }

    @Override
    public String getColumnName() {
        return getLabel() + "_" + getNumberParameter() + "_" + getCountersParameter();
    }

    @Override
    public String getSQLFragment(final String tableName, final String columnName, final DBSQLDialect dialect) {
        return getLabel() + "(" + dialect.delimit(tableName) + "." + dialect.delimit(columnName) + ", "
            + getNumberParameter() + ", " + getCountersParameter() + ") ";
    }

    @Override
    public String getSQLFragment4SubQuery(final String tableName, final String subQuery, final DBSQLDialect dialect) {
        return getLabel() + "((" + subQuery + ")" + ", " + getNumberParameter() + ", " + getCountersParameter() + ") ";
    }

    /**
     * Returns the counter parameter from the counterParameterSettings.
     *
     * @return the counter parameter from the counterParameterSettings
     */
    public Double getCountersParameter() {
        return m_countersParameterSettings.getParameter();
    }

    @Override
    public JPanel getSettingsPanel() {
        final JPanel panel = new JPanel();
        if (m_countersPrameterPanel == null) {
            m_countersPrameterPanel =
                new AdditionalNumberParameterSettingsPanel(m_countersParameterSettings, m_countersParameterLabel);
        }
        panel.add(m_countersPrameterPanel);
        return panel;
    }

    @Override
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_countersParameterSettings.loadSettingsFrom(settings);
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec spec)
        throws NotConfigurableException {
        getSettingsPanel();
        m_countersPrameterPanel.loadSettingsFrom(settings, spec);
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_countersParameterSettings.saveSettingsTo(settings);
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_countersParameterSettings.validateSettings(settings);
    }

}
