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

import java.util.Optional;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationSettings;
import org.knime.database.node.connector.SpecificDBConnectorNodeMigrationRule;
import org.knime.node.parameters.widget.credentials.Credentials;
import org.knime.workflow.migration.MigrationException;
import org.knime.workflow.migration.MigrationNodeMatchResult;
import org.knime.workflow.migration.NodeMigrationAction;
import org.knime.workflow.migration.model.MigrationNode;
import org.knime.workflow.migration.util.NodeSettingsMigrationVariablesUtilities;

/**
 * Migration rule from classic UI to modern UI Snowflake Connector node.
 *
 * @author Tobias Koetter
 */
public class SnowflakeDBConnectorNodeMigrationRule
    extends SpecificDBConnectorNodeMigrationRule<SnowflakeDBConnectorNodeSettings2, SnowflakeDBConnectorSettings> {

    @Override
    protected SnowflakeDBConnectorNodeSettings2 createNewSettings() {
        return new SnowflakeDBConnectorNodeSettings2();
    }

    @Override
    protected SnowflakeDBConnectorSettings createOldSettings() {
        return new SnowflakeDBConnectorSettings();
    }

    @Override
    protected void migrateConnectionInfo(final SnowflakeDBConnectorNodeSettings2 newSettings,
        final SnowflakeDBConnectorSettings settings) throws InvalidSettingsException {
        super.migrateConnectionInfo(newSettings, settings);
        newSettings.m_accountName = settings.getAccountName();
        newSettings.m_virtualWarehouseName = settings.getWarehouseName();
        newSettings.m_accessRole = Optional.of(settings.getRoleName());
        newSettings.m_defaultDatabase = Optional.of(settings.getDatabaseName());
        newSettings.m_defaultSchema = Optional.of(settings.getSchemaName());
    }

    @Override
    protected void migrateAuthentication(final SnowflakeDBConnectorNodeSettings2 newSettings,
        final SnowflakeDBConnectorSettings settings, final NodeSettingsWO variablesTree)
        throws InvalidSettingsException, MigrationException {
        final var authSettings = settings.getAuthenticationModel();
        if (authSettings.getAuthenticationType() == AuthenticationType.CREDENTIALS) {
            newSettings.m_authentication =
                new AuthenticationSettings(AuthenticationSettings.AuthenticationType.USER_PWD, new Credentials());
            NodeSettingsMigrationVariablesUtilities.addFlowVariable(variablesTree.addConfig("authentication"),
                "credentials", authSettings.getCredential());
        } else {
            final var authType = switch (authSettings.getAuthenticationType()) {
                case USER_PWD -> AuthenticationSettings.AuthenticationType.USER_PWD;
                case USER -> AuthenticationSettings.AuthenticationType.USER;
                case NONE -> AuthenticationSettings.AuthenticationType.NONE;
                default -> throw new MigrationException(
                    String.format("Cannot migrate settings since authentication type %s is no longer supported",
                        authSettings.getAuthenticationType()));
            };
            final var credentials = new Credentials(authSettings.getUsername(), authSettings.getPassword());
            newSettings.m_authentication = new AuthenticationSettings(authType, credentials);
        }
    }
    private static final String CONNECTION_KEY = "snowflake-connection";

    private static final String KEY_ACCOUNT_NAME_OLD = "account_name";

    private static final String KEY_WAREHOUSE_NAME_OLD = "warehouse_name";

    private static final String KEY_ROLE_NAME_OLD = "role_name";

    private static final String KEY_DATABASE_NAME_OLD = "database_name";

    private static final String KEY_SCHEMA_NAME_OLD = "schema_name";

    private static final String KEY_ACCOUNT_NAME_NEW = "accountName";

    private static final String KEY_WAREHOUSE_NAME_NEW = "virtualWarehouseName";

    private static final String KEY_ROLE_NAME_NEW = "accessRole";

    private static final String KEY_DATABASE_NAME_NEW = "defaultDatabase";

    private static final String KEY_SCHEMA_NAME_NEW = "defaultSchema";

    @Override
    protected void migrateFlowVariables(final NodeSettingsRO tree, final NodeSettingsWO variablesTree)
        throws InvalidSettingsException {
        super.migrateFlowVariables(tree, variablesTree);
        if (tree.containsKey(CONNECTION_KEY)) {
            final var connection = tree.getNodeSettings(CONNECTION_KEY);
            if (connection.containsKey(KEY_ACCOUNT_NAME_OLD)) {
                NodeSettingsMigrationVariablesUtilities.copyFlowVariable(variablesTree, KEY_ACCOUNT_NAME_NEW,
                    connection.getConfig(KEY_ACCOUNT_NAME_OLD));
            }
            if (connection.containsKey(KEY_WAREHOUSE_NAME_OLD)) {
                NodeSettingsMigrationVariablesUtilities.copyFlowVariable(variablesTree, KEY_WAREHOUSE_NAME_NEW,
                    connection.getConfig(KEY_WAREHOUSE_NAME_OLD));
            }
            if (connection.containsKey(KEY_ROLE_NAME_OLD)) {
                NodeSettingsMigrationVariablesUtilities.copyFlowVariable(variablesTree, KEY_ROLE_NAME_NEW,
                    connection.getConfig(KEY_ROLE_NAME_OLD));
            }
            if (connection.containsKey(KEY_DATABASE_NAME_OLD)) {
                NodeSettingsMigrationVariablesUtilities.copyFlowVariable(variablesTree, KEY_DATABASE_NAME_NEW,
                    connection.getConfig(KEY_DATABASE_NAME_OLD));
            }
            if (connection.containsKey(KEY_SCHEMA_NAME_OLD)) {
                NodeSettingsMigrationVariablesUtilities.copyFlowVariable(variablesTree, KEY_SCHEMA_NAME_NEW,
                    connection.getConfig(KEY_SCHEMA_NAME_OLD));
            }
        }
    }

    @Override
    protected Class<? extends NodeFactory<?>> getReplacementNodeFactoryClass(final MigrationNode migrationNode,
        final MigrationNodeMatchResult matchResult) {
        return SnowflakeDBConnectorNodeFactory2.class;
    }

    @Override
    protected MigrationNodeMatchResult match(final MigrationNode migrationNode) {
        return MigrationNodeMatchResult.of(migrationNode,
            SnowflakeDBConnectorNodeFactory.class.getName()
                .equals(migrationNode.getOriginalNodeFactoryClassName()) ? NodeMigrationAction.REPLACE : null);
    }

}
