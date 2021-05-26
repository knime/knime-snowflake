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

import org.knime.core.data.DataType;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.StringCell;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.function.aggregation.DBAggregationFunction;
import org.knime.database.function.aggregation.DBAggregationFunctionFactory;
import org.knime.database.function.aggregation.impl.functions.parameter.AbstractNumberDBAggregationFunction;

/**
 * Top K DB aggregation function.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class ApproxTopKCombinedDBAggregationFunction extends AbstractNumberDBAggregationFunction {

    private static final String LABEL = "APPROX_TOP_K_COMBINED";

    private static final int MAX_COUNTERS = 100000;

    /** Factory for the parent class. */
    public static final class Factory implements DBAggregationFunctionFactory {

        @Override
        public String getId() {
            return LABEL;
        }

        @Override
        public DBAggregationFunction createInstance() {
            return new ApproxTopKCombinedDBAggregationFunction();
        }
    }

    /**
     * Constructor.
     */
    private ApproxTopKCombinedDBAggregationFunction() {
        super("Counters: ", 1, MAX_COUNTERS, 1);
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
        return type.isCompatible(StringValue.class);
    }

    @Override
    public DataType getType(final DataType originalType) {
        return StringCell.TYPE;
    }

    @Override
    public String getDescription() {
        return "This allows scenarios where APPROX_TOP_K_ACCUMULATE is run over horizontal partitions of the same "
            + "table, producing an algorithm state for each table partition. These states can later be combined "
            + "using APPROX_TOP_K_COMBINE, producing the same output state as a single run of APPROX_TOP_K_ACCUMULATE "
            + "over the entire table.";
    }

    @Override
    public String getColumnName() {
        return getLabel() + "_" + getNumberParameter();
    }

    @Override
    public String getSQLFragment(final String tableName, final String columnName, final DBSQLDialect dialect) {
        return getLabel() + "(" + dialect.delimit(tableName) + "." + dialect.delimit(columnName) + ", "
            + getNumberParameter() + ") ";
    }

    @Override
    public String getSQLFragment4SubQuery(final String tableName, final String subQuery, final DBSQLDialect dialect) {
        return getLabel() + "((" + subQuery + ")" + ", " + getNumberParameter() + ") ";
    }

}
