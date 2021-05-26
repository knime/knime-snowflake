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

package org.knime.database.extension.snowflake.agent;

import org.knime.database.agent.metadata.impl.DefaultDBMetadataReader;
import org.knime.database.session.DBSessionReference;

/**
 * Snowflake database metadata reader.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeDBMetadataReader extends DefaultDBMetadataReader {

    /**
     * Constructs an {@link SnowflakeDBMetadataReader} object.
     *
     * @param sessionReference the reference to the agent's session.
     */
    public SnowflakeDBMetadataReader(final DBSessionReference sessionReference) {
        super(sessionReference);
    }
//
//    @Override
//    protected SQLType getSqlType(final ExecutionMonitor exec, final int columnType, final String columnTypeName)
//        throws CanceledExecutionException, SQLException {
//        /*SnowflakeType specific types as of Snowflake 19c not covered by the switch below
//        CURSOR              -10
//        FIXED_CHAR          999 Use this type when binding to a CHAR column in the where clause of a Select statement
//        INTERVALDS          -104
//        INTERVALYM          -103
//        JAVA_STRUCT         2008
//        OPAQUE              2007
//        PLSQL_INDEX_TABLE   -14
//        TIMESTAMPLTZ        -102
//         */
//        switch (columnType) {
//            case -101: //TIMESTAMPTZ
//                return TIMESTAMP_WITH_TIMEZONE;
//            case -100: //TIMESTAMPNS deprecated since 9.2.0. Use SnowflakeTypes.TIMESTAMP instead
//                return TIMESTAMP;
//            case -2: //RAW shares same value as BINARY as it is synonym
//                return BINARY;
//            case -13: //BFILE is a BLOB stored in an external file
//                return BLOB;
//            case 2: //NUMBER shares same value as NUMERIC as it is synonym
//                return NUMERIC;
//            case 100: //BINARY_FLOAT
//                return FLOAT;
//            case 101: //BINARY_DOUBLE
//                return DOUBLE;
//            case 252: //PLSQL_BOOLEAN binds BOOLEAN type for input/output parameters when executing a PLSQL procedure
//                return BOOLEAN;
//            case 2007: //OPAQUE An opaque type is one whose internal structure is not known to the database
//                return OTHER;
//            default:
//                return super.getSqlType(exec, columnType, columnTypeName);
//        }
//    }
}
