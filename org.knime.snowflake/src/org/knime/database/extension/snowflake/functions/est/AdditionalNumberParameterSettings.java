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

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleBounded;
import org.knime.database.function.aggregation.impl.functions.parameter.SelectFunctionSettingsPanel;

/**
 * Class that saves the settings of the {@link SelectFunctionSettingsPanel}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class AdditionalNumberParameterSettings {

    private final SettingsModelDoubleBounded m_number;

    /**
     * Constructor.
     * @param key the unique config key
     * @param min the parameter's minimum value
     * @param max the parameter's maximum value
     * @param def the default parameter
     */
    public AdditionalNumberParameterSettings(final String key, final double min, final double max, final double def) {
        m_number = new SettingsModelDoubleBounded(key, def, min, max);
    }

    /**
     * Returns the model.
     * @return the boolean model
     */
    SettingsModelDoubleBounded getModel() {
        return m_number;
    }

    /**
     * Validates the settings.
     * @param settings the {@link NodeSettingsRO} to read the settings from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_number.validateSettings(settings);
    }

    /**
     * Loads the settings.
     * @param settings the {@link NodeSettingsRO} to read the settings from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_number.loadSettingsFrom(settings);
    }

    /**
     * Saves the settings.
     * @param settings the {@link NodeSettingsWO} to write to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_number.saveSettingsTo(settings);
    }

    /**
     * Returns the parameter value.
     * @return the models value
     */
    public Double getParameter() {
        return m_number.getDoubleValue();
    }

    /**
     * Returns the minimum allowed parameter value.
     * @return the model's minimum value
     */
    public Double getMin() {
        return m_number.getLowerBound();
    }

    /**
     * Returns the maximum allowed parameter value.
     * @return the models maximum value
     */
    public Double getMax() {
        return m_number.getUpperBound();
    }

    /**
     * Returns a clone.
     * @return a clone of this settings object
     */
    public AdditionalNumberParameterSettings createClone() {
        return new AdditionalNumberParameterSettings(getModel().getKey(), getMin(), getMax(), getParameter());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + getModel().getKey().hashCode();
        result = prime * result + ((m_number == null) ? 0 : m_number.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AdditionalNumberParameterSettings other = (AdditionalNumberParameterSettings)obj;
        if (!getModel().getKey().equals(other.getModel().getKey())) {
            return false;
        }
        if (m_number == null) {
            if (other.m_number != null) {
                return false;
            }
        } else if (!m_number.equals(other.m_number)) {
            return false;
        }
        return true;
    }
}
