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

package org.knime.database.extension.snowflake.functions;

import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.DoubleCell;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.function.aggregation.impl.functions.parameter.AbstractNumberDBAggregationFunction;

/**
 * Abstract Snowflake percentile aggregation function.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractPercentileDBAggregationFunction extends AbstractNumberDBAggregationFunction {

    private static final double DEFAULT_PERCENTILE = 0.1;

    private final String m_label;

    private final String m_description;

    private Class<? extends DataValue> m_compatibleClass;

    /**
     * Constructor.
     *
     * @param label the label of the function
     * @param description the description
     */
    public AbstractPercentileDBAggregationFunction(final String label, final String description) {
        this(label, description, DoubleValue.class);
    }

    /**
     * Constructor.
     *
     * @param label the label of the function
     * @param description the description
     * @param valueClass the compatible DataValue class
     */
    public AbstractPercentileDBAggregationFunction(final String label, final String description,
        final Class<? extends DataValue> valueClass) {
        super("Percentile: ", 0, 1, DEFAULT_PERCENTILE);
        m_label = label;
        m_description = description;
        m_compatibleClass = valueClass;
    }

    @Override
    public String getId() {
        return m_label;
    }

    @Override
    public String getLabel() {
        return m_label;
    }

    @Override
    public boolean isCompatible(final DataType type) {
        return type.isCompatible(m_compatibleClass);
    }

    @Override
    public DataType getType(final DataType originalType) {
        return DoubleCell.TYPE;
    }

    @Override
    public String getDescription() {
        return m_description;
    }

    @Override
    public String getColumnName() {
        return getLabel() + "_" + getNumberParameter();
    }

    @Override
    public String getSQLFragment(final String tableName, final String columnName, final DBSQLDialect dialect) {
        return getLabel() + "(" + getNumberParameter() + ") " + "WITHIN GROUP (ORDER BY " + dialect.delimit(tableName)
            + "." + dialect.delimit(columnName) + ")";
    }

    @Override
    public String getSQLFragment4SubQuery(final String tableName, final String subQuery, final DBSQLDialect dialect) {
        return getLabel() + "(" + getNumberParameter() + ") WITHIN GROUP (ORDER BY " + subQuery + ")";
    }

}
