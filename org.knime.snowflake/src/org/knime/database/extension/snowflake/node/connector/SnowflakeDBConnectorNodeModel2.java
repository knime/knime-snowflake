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
 *   May 20, 2025 (Martin Sillye, TNG Technology Consulting GmbH): created
 */
package org.knime.database.extension.snowflake.node.connector;

import static org.apache.commons.lang3.StringUtils.stripToEmpty;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_DATABASE;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_HOST;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_PORT;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_SCHEMA;
import static org.knime.database.driver.URLTemplates.resolveDriverUrl;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ACCOUNT_DOMAIN;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ACCOUNT_NAME;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ROLE;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_WAREHOUSE;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_DRIVER_URL_TEMPLATE_IS_INVALID;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_HOST_IS_NOT_DEFINED;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_NAME_IS_NOT_DEFINED;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_PORT_IS_NOT_DEFINED;
import static org.knime.database.node.connector.ConnectorMessages.DATABASE_SCHEMA_IS_NOT_DEFINED;
import static org.knime.database.node.connector.server.UnauthenticatedServerDBConnectorNodeModel2.LOGGER;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObject;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationSettings;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.credentials.base.oauth.api.AccessTokenAccessor;
import org.knime.credentials.base.oauth.api.JWTCredential;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.connection.UserDBConnectionController;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.extension.snowflake.type.Snowflake;
import org.knime.database.node.connector.server.UnauthenticatedServerDBConnectorNodeModel2;
import org.knime.database.util.BlankTokenValueException;
import org.knime.database.util.NestedTokenException;
import org.knime.database.util.NoSuchTokenException;
import org.knime.database.util.StringTokenException;

/**
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 * @since 5.8
 */
@SuppressWarnings("restriction")
final class SnowflakeDBConnectorNodeModel2
    extends UnauthenticatedServerDBConnectorNodeModel2<SnowflakeDBConnectorNodeSettings> {

    SnowflakeDBConnectorNodeModel2(final PortsConfiguration configuration) {
        super(Snowflake.DB_TYPE, configuration, SnowflakeDBConnectorNodeSettings.class);
    }

    @Override
    protected DBConnectionController createConnectionController(final SnowflakeDBConnectorNodeSettings modelSettings)
        throws InvalidSettingsException {
        final var credentials = modelSettings.m_authentication.getCredentials();
        return new UserDBConnectionController(getDBUrl(modelSettings),
            getAuthenticationType(modelSettings.m_authentication), credentials.getUsername(), credentials.getPassword(),
            null, null, modelSettings.m_host);
    }

    private static AuthenticationType getAuthenticationType(final AuthenticationSettings authSettings) {
        return switch (authSettings.getType()) {
            case NONE -> AuthenticationType.NONE;
            case KERBEROS -> AuthenticationType.KERBEROS;
            case USER_PWD -> AuthenticationType.USER_PWD;
            default -> throw new IllegalArgumentException("Unknown Authentication type");
        };
    }

    private DBConnectionController createConnectionControllerWithInput(final CredentialPortObjectSpec credentialSpec,
        final SnowflakeDBConnectorNodeSettings modelSettings) throws InvalidSettingsException {

        final JWTCredential credential;
        try {
            credential = credentialSpec.resolveCredential(JWTCredential.class);
        } catch (NoSuchCredentialException ex) {
            throw new InvalidSettingsException(ex.getMessage());
        }

        return new MSAuthDBConnectionController((AccessTokenAccessor)credential, getDBUrl(modelSettings));
    }

    @Override
    protected DBConnectionController createConnectionController(final List<PortObject> inObjects,
        final SnowflakeDBConnectorNodeSettings sessionSettings, final ExecutionMonitor monitor)
        throws InvalidSettingsException {
        if (!inObjects.isEmpty() && inObjects.get(0) instanceof CredentialPortObject cred) {
            try {
                final var spec = (CredentialPortObjectSpec)inObjects.get(0).getSpec();
                final AccessTokenAccessor tokenAccessor = spec.toAccessor(AccessTokenAccessor.class);
                return new SnowflakeOAuthDBConnectionController(tokenAccessor, getDBUrl(sessionSettings));
            } catch (NoSuchCredentialException ex) {
                throw new InvalidSettingsException(ex.getMessage(), ex);
            }
        }
        final SettingsModelAuthentication authentication = getSettings().getAuthenticationModel();
        final var credentialsProvider = getCredentialsProvider();
        return new UserDBConnectionController(getDBUrl(sessionSettings), authentication.getAuthenticationType(),
            authentication.getUserName(credentialsProvider), authentication.getPassword(credentialsProvider),
            authentication.getCredential(), credentialsProvider);
    }

    @Override
    protected void validateSessionInfoSettings(final SnowflakeDBConnectorNodeSettings modelSettings)
        throws InvalidSettingsException {
        modelSettings.validateAuthenticationSettings();
    }
    /**
     * @param modelSettings settings of the node
     * @return the generated URL
     * @throws InvalidSettingsException
     */
    @Override
    protected String getDBUrl(final SnowflakeDBConnectorNodeSettings settings) throws InvalidSettingsException {
        final Optional<DBDriverWrapper> driver = DBDriverRegistry.getInstance().getDriver(settings.getDriver());
        if (!driver.isPresent()) {
            return null;
        }
        final Map<String, String> variableValues = new HashMap<>();
        variableValues.put(VARIABLE_NAME_ACCOUNT_NAME, stripToEmpty(settings.getAccountName()));
        variableValues.put(VARIABLE_NAME_ACCOUNT_DOMAIN, stripToEmpty(settings.getAccountDomain()));
        variableValues.put(VARIABLE_NAME_WAREHOUSE, stripToEmpty(settings.getWarehouseName()));
        variableValues.put(VARIABLE_NAME_ROLE, stripToEmpty(settings.getRoleName()));
        variableValues.put(VARIABLE_NAME_DATABASE, stripToEmpty(settings.getDatabaseName()));
        variableValues.put(VARIABLE_NAME_SCHEMA, stripToEmpty(settings.getSchemaName()));
        try {
            return resolveDriverUrl(driver.get().getURLTemplate(), Collections.emptyMap(), variableValues);
        } catch (final BlankTokenValueException exception) {
            final String token = exception.getToken();
            String message = exception.getMessage();
            if (token != null) {
                switch (token) {
                    case VARIABLE_NAME_ACCOUNT_NAME:
                        message = SnowflakeDBConnectorSettings.ACCOUNT_NAME_IS_NOT_DEFINED;
                        break;
                    case VARIABLE_NAME_WAREHOUSE:
                        message = SnowflakeDBConnectorSettings.WAREHOUSE_NAME_IS_NOT_DEFINED;
                        break;
                    case VARIABLE_NAME_ROLE:
                        message = SnowflakeDBConnectorSettings.ROLE_NAME_IS_NOT_DEFINED;
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
