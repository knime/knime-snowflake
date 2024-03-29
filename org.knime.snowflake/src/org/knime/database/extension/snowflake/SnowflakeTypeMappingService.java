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

package org.knime.database.extension.snowflake;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.time.localtime.LocalTimeCellFactory;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.database.datatype.mapping.AbstractDBDataTypeMappingService;

/**
 * Database type mapping service.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class SnowflakeTypeMappingService
    extends AbstractDBDataTypeMappingService<SnowflakeSource, SnowflakeDestination> {

    private static final SnowflakeTypeMappingService INSTANCE = new SnowflakeTypeMappingService();

    /**
     * Gets the singleton {@link SnowflakeTypeMappingService} instance.
     *
     * @return the only {@link SnowflakeTypeMappingService} instance.
     */
    public static SnowflakeTypeMappingService getInstance() {
        return INSTANCE;
    }

    private SnowflakeTypeMappingService() {
        super(SnowflakeSource.class, SnowflakeDestination.class);

        // Default consumption paths
        final Map<DataType, Triple<DataType, Class<?>, SQLType>> defaultConsumptionMap =
            new LinkedHashMap<>(getDefaultConsumptionTriples());
        addTriple(defaultConsumptionMap, BooleanCell.TYPE, Boolean.class, JDBCType.BOOLEAN);
        //int is also mapped to bigint in snowflake
        //https://docs.snowflake.com/en/sql-reference/data-types-numeric.html
        //#int-integer-bigint-smallint-tinyint-byteint
        addTriple(defaultConsumptionMap, IntCell.TYPE, Long.class, JDBCType.BIGINT);
        //need to prevent problems with the time type (see AP-16726)
        addTriple(defaultConsumptionMap, LocalTimeCellFactory.TYPE, String.class, JDBCType.TIME);
        addTriple(defaultConsumptionMap, ZonedDateTimeCellFactory.TYPE, ZonedDateTime.class,
            JDBCType.TIMESTAMP_WITH_TIMEZONE);
        setDefaultConsumptionTriples(defaultConsumptionMap);

        // Default production paths
        final Map<SQLType, Triple<SQLType, Class<?>, DataType>> defaultProductionMap = getDefaultProductionTriples();
        //need to prevent problems with the time type (see AP-16726)
        addTriple(defaultProductionMap, JDBCType.TIME, String.class, LocalTimeCellFactory.TYPE);
        setDefaultProductionTriples(defaultProductionMap);

        //https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#timestamp-ltz-timestamp-ntz-timestamp-tz
        //the default precision does not seem to be 9 which is why we specify it to retain all digits
        //TIMESTAMP is mapped to one of the internal types based on the TIMESTAMP_TYPE_MAPPING parameter
        //which by default is timestamp_ntz:
        //https://docs.snowflake.com/en/sql-reference/parameters.html#label-timestamp-type-mapping
        addColumnType(JDBCType.TIMESTAMP, "timestamp(9)");

        //https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#timestamp-ltz-timestamp-ntz-timestamp-tz
        addColumnType(JDBCType.TIMESTAMP_WITH_TIMEZONE, "timestamp_tz");

        //https://docs.snowflake.com/en/sql-reference/data-types-text.html#varchar
        //If a length is not specified, the default is the maximum length.
        //A column only consumes storage for the amount of actual data stored
        addColumnType(JDBCType.VARCHAR, "varchar");
    }

}
