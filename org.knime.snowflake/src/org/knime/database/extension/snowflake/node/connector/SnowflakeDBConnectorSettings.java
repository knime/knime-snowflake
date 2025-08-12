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

package org.knime.database.extension.snowflake.node.connector;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.stripToEmpty;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_DATABASE;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_SCHEMA;
import static org.knime.database.driver.URLTemplates.resolveDriverUrl;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.ATTRIBUTES;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.ATTRIBUTE_ACCOUNT_DOMAIN;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ACCOUNT_DOMAIN;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ACCOUNT_NAME;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ROLE;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_WAREHOUSE;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_DRIVER_URL_IS_BLANK;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_DRIVER_URL_TEMPLATE_IS_INVALID;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_NAME_IS_NOT_DEFINED;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_SCHEMA_IS_NOT_DEFINED;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.database.DBType;
import org.knime.database.VariableContext;
import org.knime.database.attribute.AttributeValueRepository;
import org.knime.database.dialect.DBSQLDialectRegistry;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.extension.snowflake.type.Snowflake;
import org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator;
import org.knime.database.node.connector.DBSessionSettings;
import org.knime.database.util.BlankTokenValueException;
import org.knime.database.util.NestedTokenException;
import org.knime.database.util.NoSuchTokenException;
import org.knime.database.util.StringTokenException;

/**
 * Settings model for the <em>Snowflake connector node</em>.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeDBConnectorSettings extends DBSessionSettings {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SnowflakeDBConnectorSettings.class);

    private static final DBType DB_TYPE = Snowflake.DB_TYPE;

    private static final String CONFIG_NAME = "snowflake-connection";

    private static final String KEY_ACCOUNT_NAME = "account_name";

    private static final String KEY_WAREHOUSE_NAME = "warehouse_name";

    private static final String KEY_ROLE_NAME = "role_name";

    private static final String KEY_DATABASE_NAME = "database_name";

    private static final String KEY_SCHEMA_NAME = "schema_name";

    private static final String CONFIG_AUTHENTICATION = "authentication";

    /** Message when account name was not defined. */
    public static final String ACCOUNT_NAME_IS_NOT_DEFINED = "Account name is not defined.";

    /** Message when warehouse name was not defined. */
    public static final String WAREHOUSE_NAME_IS_NOT_DEFINED = "Warehouse name is not defined.";

    /** Message when role name was not defined. */
    public static final String ROLE_NAME_IS_NOT_DEFINED = "Role name is not defined.";

    private String m_accountName;

    private String m_warehouseName;

    private String m_roleName = "PUBLIC";

    private String m_databaseName;

    private String m_schemaName;

    private final SettingsModelAuthentication m_authentication =
        new SettingsModelAuthentication(CONFIG_AUTHENTICATION, AuthenticationType.USER_PWD);

    /**
     * Constructs an {@link SnowflakeDBConnectorSettings} object.
     */
    public SnowflakeDBConnectorSettings() {
        setDBType(DB_TYPE.getId());
        setDialect(DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(DB_TYPE).getId());
        final DBDriverWrapper defaultDriver = DBDriverRegistry.getInstance().getLatestDriver(DB_TYPE);
        setDriver(defaultDriver == null ? null : defaultDriver.getDriverDefinition().getId());
    }

    static String getDBUrl(final String driverName, final String accountName, final String accountDomain,
        final String warehouseName, final String roleName, final String databaseName, final String schemaName)
        throws InvalidSettingsException {
        final Optional<DBDriverWrapper> driver = DBDriverRegistry.getInstance().getDriver(driverName);
        if (!driver.isPresent()) {
            return null;
        }
        final Map<String, String> variableValues = new HashMap<>();
        variableValues.put(VARIABLE_NAME_ACCOUNT_NAME, stripToEmpty(accountName));
        variableValues.put(VARIABLE_NAME_ACCOUNT_DOMAIN, stripToEmpty(accountDomain));
        variableValues.put(VARIABLE_NAME_WAREHOUSE, stripToEmpty(warehouseName));
        variableValues.put(VARIABLE_NAME_ROLE, stripToEmpty(roleName));
        variableValues.put(VARIABLE_NAME_DATABASE, stripToEmpty(databaseName));
        variableValues.put(VARIABLE_NAME_SCHEMA, stripToEmpty(schemaName));
        try {
            return resolveDriverUrl(driver.get().getURLTemplate(), Collections.emptyMap(), variableValues);
        } catch (final BlankTokenValueException exception) {
            final String token = exception.getToken();
            String message = exception.getMessage();
            if (token != null) {
                switch (token) {
                    case VARIABLE_NAME_ACCOUNT_NAME:
                        message = ACCOUNT_NAME_IS_NOT_DEFINED;
                        break;
                    case VARIABLE_NAME_WAREHOUSE:
                        message = WAREHOUSE_NAME_IS_NOT_DEFINED;
                        break;
                    case VARIABLE_NAME_ROLE:
                        message = ROLE_NAME_IS_NOT_DEFINED;
                        break;
                    case VARIABLE_NAME_DATABASE:
                        message = DATABASE_NAME_IS_NOT_DEFINED;
                        break;
                    case VARIABLE_NAME_SCHEMA:
                        message = DATABASE_SCHEMA_IS_NOT_DEFINED;
                        break;
                    default:
                        LOGGER.codingWithFormat(
                            "There is no alternative error message for the blank mandatory token: \"%s\"", token);
                }
            }
            throw new InvalidSettingsException(message, exception);
        } catch (final NestedTokenException exception) {
            final String token = exception.getToken();
            throw new InvalidSettingsException(
                "The token " + (token == null ? null : '"' + token + '"') + " has illegally nested content.",
                exception);
        } catch (final NoSuchTokenException exception) {
            final String token = exception.getToken();
            throw new InvalidSettingsException((token == null ? null : '"' + token + '"')
                + " is not a valid driver URL template token. Please refer to the node documentation for the available"
                + " URL template tokens depending on the chosen settings.", exception);
        } catch (final StringTokenException exception) {
            throw new InvalidSettingsException(DATABASE_DRIVER_URL_TEMPLATE_IS_INVALID, exception);
        }

    }

    @Override
    public String getDBUrl() throws InvalidSettingsException {
        return getDBUrl(getDriver(), getAccountName(), getAccountDomain(), getWarehouseName(), getRoleName(),
            getDatabaseName(), getSchemaName());
    }

    /**
     * Gets the database account name.
     *
     * @return the account name of the database.
     */
    public String getAccountName() {
        return m_accountName;
    }

    /**
     * Gets the account domain which is typically snowflakecomputing.com.
     *
     * @return the domain part of the Snowflake account
     */
    private String getAccountDomain() {
        final AttributeValueRepository valueRepository = AttributeValueRepository.builder()
            .withAttributeCollection(ATTRIBUTES).withMappings(getAttributeValues()).build();
        String domain = valueRepository.get(ATTRIBUTE_ACCOUNT_DOMAIN);
        if (domain == null) {
            domain = SnowflakeAbstractDriverLocator.DEFAULT_ACCOUNT_DOMAIN.toString();
        }
        return domain;
    }

    /**
     * Sets the database account name.
     *
     * @param accountName the database account name to set
     */
    public void setAccountName(final String accountName) {
        m_accountName = accountName;
    }

    /**
     * Gets the warehouse name.
     *
     * @return the warehouse
     */
    public String getWarehouseName() {
        return m_warehouseName;
    }

    /**
     * Sets the warehouse name.
     *
     * @param warehouse the warehouse to set
     */
    public void setWarehouse(final String warehouse) {
        m_warehouseName = warehouse;
    }

    /**
     * Gets the role name.
     *
     * @return the role name.
     */
    public String getRoleName() {
        return m_roleName;
    }

    /**
     * Sets the role name.
     *
     * @param roleName the role name to set
     */
    public void setRoleName(final String roleName) {
        m_roleName = roleName;
    }

    /**
     * Gets the database name.
     *
     * @return the name of database
     */
    public String getDatabaseName() {
        return m_databaseName;
    }

    /**
     * Sets the database name.
     *
     * @param databaseName the database name to set
     */
    public void setDatabaseName(final String databaseName) {
        m_databaseName = databaseName;
    }

    /**
     * Gets the schema name.
     *
     * @return the name of schema
     */
    public String getSchemaName() {
        return m_schemaName;
    }

    /**
     * Sets the schema name.
     *
     * @param schemaName the schema name to set
     */
    public void setSchemaName(final String schemaName) {
        m_schemaName = schemaName;
    }

    /**
     * Gets the {@linkplain SettingsModelAuthentication authentication model} for the node.
     *
     * @return the {@linkplain SettingsModelAuthentication authentication model}
     */
    public SettingsModelAuthentication getAuthenticationModel() {
        return m_authentication;
    }

    //************************* Save, Validate & Load Settings *************************

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        saveSettingsToConfig(settings.addConfig(CONFIG_NAME));
        m_authentication.saveSettingsTo(settings);
    }

    @Override
    public void validate(final VariableContext variableContext) throws InvalidSettingsException {
        super.validate(variableContext);
        if (isBlank(getDBUrl())) {
            throw new InvalidSettingsException(DATABASE_DRIVER_URL_IS_BLANK);
        }
        if (isBlank(m_warehouseName)) {
            throw new InvalidSettingsException("Please enter a warehouse name");
        }
        final NodeSettings authenticationSettings = new NodeSettings("temporary");
        m_authentication.saveSettingsTo(authenticationSettings);
        m_authentication.validateSettings(authenticationSettings);
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        // The URL is not validated because the template may change independently from the node's lifecycle.
        m_authentication.validateSettings(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsFrom(settings);
        loadSettingsFromConfig(settings.getConfig(CONFIG_NAME));
        m_authentication.loadSettingsFrom(settings);
    }

    /**
     * Loads the server-based connector settings from the {@linkplain ConfigWO configuration}.
     * <p>
     * This method is recommended to be extended when additional parameters should be loaded by a subclass.
     * </p>
     *
     * @param config the configuration object to load node settings from.
     * @throws InvalidSettingsException if any of the settings is not valid.
     */
    protected void loadSettingsFromConfig(final ConfigRO config) throws InvalidSettingsException {
        m_accountName = config.getString(KEY_ACCOUNT_NAME);
        m_warehouseName = config.getString(KEY_WAREHOUSE_NAME);
        m_roleName = config.getString(KEY_ROLE_NAME);
        m_databaseName = config.getString(KEY_DATABASE_NAME);
        m_schemaName = config.getString(KEY_SCHEMA_NAME);
    }

    /**
     * Saves the server-based connector settings to the {@linkplain ConfigWO configuration}.
     * <p>
     * The method must be extended when additional parameters should be stored by a subclass.
     * </p>
     *
     * @param config the configuration object to save node settings to.
     */
    protected void saveSettingsToConfig(final ConfigWO config) {
        config.addString(KEY_ACCOUNT_NAME, m_accountName);
        config.addString(KEY_WAREHOUSE_NAME, m_warehouseName);
        config.addString(KEY_ROLE_NAME, m_roleName);
        config.addString(KEY_DATABASE_NAME, m_databaseName);
        config.addString(KEY_SCHEMA_NAME, m_schemaName);
    }

}
