/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   10 May 2021 (Tobias): created
 */
package org.knime.snowflake.testing.janitor;

import java.time.LocalDateTime;
import java.util.List;

import org.knime.core.node.workflow.FlowVariable;
import org.knime.database.DBType;
import org.knime.database.extension.snowflake.type.Snowflake;
import org.knime.database.testing.janitor.api.AbstractDatabaseJanitor;

/**
 * Snowflake database test janitor.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeDatabaseJanitor extends AbstractDatabaseJanitor {

    private static final String WAREHOUSE = "KNIME_WORKLFOW_TESTS";

    private static final String ROLE = "KNIME_JENKINS";

    private static final String INITIAL_SCHEMA = "KNIME_TESTING";

    private static final String DATABASE = "KNIME_JENKINS";

    private static final int DB_PORT = -1;

    private static final String DATABASE_TYPE = Snowflake.DB_TYPE.getId();

    //Copied from SnowflakeDriverLocator
    private static final String DRIVER_CLASS = "net.snowflake.client.jdbc.SnowflakeDriver";
    private static final String DRIVER_ID = "built-in-snowflake-3.13.3";

    /**
     * Constructor.
     */
    public SnowflakeDatabaseJanitor() {
        super(INITIAL_SCHEMA, null, DB_PORT, null, null, DATABASE_TYPE, DATABASE_TYPE,
            DRIVER_ID, DRIVER_CLASS, createNewSchemaName(), DATABASE);
    }

    private static String createNewSchemaName() {
        return "KNIME_TESTING_" + DATE_FORMAT.format(LocalDateTime.now()) + "_" + Integer.toHexString(RAND.nextInt());
    }

    @Override
    public List<FlowVariable> getFlowVariables() {
        final List<FlowVariable> flowVariables = super.getFlowVariables();
        final String variablePrefix = getVariablePrefix();
        flowVariables.add(new FlowVariable(variablePrefix + "account-name", getHost()));
        flowVariables.add(new FlowVariable(variablePrefix + "warehouse", WAREHOUSE));
        flowVariables.add(new FlowVariable(variablePrefix + "role", ROLE));
        return flowVariables;
    }

    @Override
    protected String getNewDBName() {
        return DATABASE;
    }

    @Override
    protected void updateDatabaseInfo() {
        setDefaultSchema(createNewSchemaName());
    }

    @Override
    protected String getCreateDatabaseStatement(final String dbName) {
        return "CREATE OR REPLACE TRANSIENT SCHEMA \"" + getDefaultSchema() + "\"" +
                " DATA_RETENTION_TIME_IN_DAYS=0" +
                " COMMENT='Schema used for workflow tests.'";
    }

    @Override
    protected String getDropDatabaseStatement(final String dbName) {
        return "DROP SCHEMA \"" + getDefaultSchema() + "\"";
    }

    @Override
    protected String getJdbcUrl(final String dbName) {
        return "jdbc:snowflake://" + getHost() + ".snowflakecomputing.com/" +
                "?warehouse=" + WAREHOUSE +
                "&role=" + ROLE +
                "&db=" + DATABASE; // no default schema since this is what we will create later on
    }

    @Override
    protected DBType getDBType() {
        return Snowflake.DB_TYPE;
    }

    @Override
    public String getName() {
        return "Snowflake test database";
    }

    @Override
    public String getID() {
        return "org.knime.snowflake.testing.janitor.SnowflakeDatabaseJanitor";
    }

}
