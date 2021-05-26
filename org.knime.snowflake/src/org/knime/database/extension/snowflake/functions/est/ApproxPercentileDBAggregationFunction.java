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

import org.knime.database.extension.snowflake.functions.AbstractPercentileDBAggregationFunction;
import org.knime.database.function.aggregation.DBAggregationFunction;
import org.knime.database.function.aggregation.DBAggregationFunctionFactory;

/**
 * Approximate percentile db aggregation function.
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class ApproxPercentileDBAggregationFunction extends AbstractPercentileDBAggregationFunction {

    private static final String LABEL = "APPROX_PERCENTILE";

    /** Factory for the parent class. */
    public static final class Factory implements DBAggregationFunctionFactory {

        @Override
        public String getId() {
            return LABEL;
        }

        @Override
        public DBAggregationFunction createInstance() {
            return new ApproxPercentileDBAggregationFunction();
        }
    }

    private ApproxPercentileDBAggregationFunction() {
        super(LABEL, "Returns an approximated value for the desired percentile (i.e. if column c has n numbers, "
            + "then APPROX_PERCENTILE(c, p) returns a number such that approximately n * p of the numbers in c "
            + "are smaller than the returned number).");
    }

}
