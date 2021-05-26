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

package org.knime.snowflake.tests.collector;

import static org.knime.database.testing.framework.DBProperties.DATABASE;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.knime.database.DBType;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.extension.snowflake.dialect.SnowflakeDBSQLDialect;
import org.knime.database.extension.snowflake.type.Snowflake;
import org.knime.database.testing.framework.DBProperties;
import org.knime.database.testing.framework.db.AbstractBuilder;
import org.knime.database.testing.framework.db.AbstractDatabase;
import org.knime.database.testing.framework.db.AbstractDatabaseBuilderFactory;
import org.knime.database.testing.framework.validator.DefaultDBValidator;

/**
 * Snowflake database wrapper class.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class SnowflakeDatabase extends AbstractDatabase<DefaultDBValidator> {

    private static final DBType DB_TYPE = Snowflake.DB_TYPE;

    private static final String DEFAULT_DIALECT = SnowflakeDBSQLDialect.ID;

    private static final String DEFAULT_DRIVER =
        DBDriverRegistry.getInstance().getLatestDriver(DB_TYPE).getDriverDefinition().getId();

    private String m_schemaName;

    private String m_databaseName;

    @Override
    protected void cleanDatabase(final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(getDBProperties().get(DBProperties.SCRIPT_RESET_DB, getId()));
        }
    }

    private SnowflakeDatabase(final DatabaseVO databaseVO, final DBProperties properties, final String databaseName,
        final String schemaName) throws SQLException {
        super(databaseVO, DefaultDBValidator::new, properties);
        m_databaseName = databaseName;
        m_schemaName = schemaName;
    }

    /**
     * Gets the database that is used during the test run.
     *
     * @return the databaseName
     */
    public String getDatabaseName() {
        return m_databaseName;
    }


    /**
     * Gets the schema that is used during the test run.
     *
     * @return the testing schema
     */
    public String getSchemaName() {
        return m_schemaName;
    }

    /**
     * Creates a Snowflake database builder object.
     *
     * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
     */
    public static final class SnowflakeBuilderFactory extends AbstractDatabaseBuilderFactory {

        /**
         * Constructor used in extension point registry.
         */
        public SnowflakeBuilderFactory() {
            super(DB_TYPE, DEFAULT_DIALECT, DEFAULT_DRIVER);
        }

        @Override
        public SnowflakeBuilder get() {
            return new SnowflakeBuilder(getDialect(), getDriverID());
        }
    }

    /**
     * Builder for the Snowflake database object.
     *
     * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
     */
    public static final class SnowflakeBuilder
        extends AbstractBuilder<SnowflakeBuilder, SnowflakeDatabase, DefaultDBValidator> {

        private String m_accountName;

        private String m_warehouse;

        private String m_role;

        private String m_database;

        private String m_schema;

        @Override
        public SnowflakeBuilder withDBProperties(final DBProperties properties) {
            super.withDBProperties(properties);
            m_accountName = properties.get("account", getId());
            if (m_accountName.equals("<account_name>")) {
                throw new RuntimeException("Snowflake account name is not specified!");
            }
            m_warehouse = properties.get("warehouse", getId());
            m_role = properties.get("role", getId());
            m_database = properties.get(DATABASE, getId());
            m_schema = properties.get("schema", getId());
            return this;
        }

        private SnowflakeBuilder(final String dialect, final String driver) {
            super(DB_TYPE, dialect, driver);
        }


        @Override
        public SnowflakeDatabase build() throws SQLException {
            final String url = "jdbc:snowflake://" + m_accountName + ".snowflakecomputing.com/" //forced line break
                    + "?warehouse=" + m_warehouse //forced line break
                    + "&role=" + m_role  //forced line break
                    + "&db=" + m_database  //forced line break
                    + "&schema=" + m_schema
                    + "&JDBC_USE_SESSION_TIMEZONE=false";
            return new SnowflakeDatabase(new DatabaseVO(getType(), getDialect(), getDriver(), url, getJdbcProperties()),
                getDBProperties(), m_database, m_schema);
        }
    }

}
