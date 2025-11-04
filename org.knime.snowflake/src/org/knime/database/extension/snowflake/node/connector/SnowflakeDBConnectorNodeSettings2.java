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

import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.ATTRIBUTES;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.ATTRIBUTE_ACCOUNT_DOMAIN;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationFromInPortUtil.AuthenticationManagedByPortMessage;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationFromInPortUtil.HasCredentialPort;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationSettings;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.AuthenticationSettings.AuthenticationType;
import org.knime.core.webui.node.dialog.defaultdialog.setting.credentials.BaseAuthenticationSettings.AuthenticationTypeModification;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.database.attribute.AttributeValueRepository;
import org.knime.database.driver.DBDriverDefinition;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.extension.snowflake.dialect.SnowflakeDBSQLDialect;
import org.knime.database.extension.snowflake.node.connector.SnowflakeDBConnectorNodeSettings2.ConnectorModification;
import org.knime.database.extension.snowflake.type.Snowflake;
import org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator;
import org.knime.database.node.connector.DBConnectorNodeSettingsUtils.DBDialectChoicesProvider;
import org.knime.database.node.connector.DBConnectorNodeSettingsUtils.DBDriverChoicesProvider;
import org.knime.database.node.connector.SpecificDBConnectorNodeSettings;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.widget.choices.EnumChoicesProvider;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.credentials.Credentials;
import org.knime.node.parameters.widget.message.TextMessage;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidgetValidation.PatternValidation.IsNotBlankValidation;

/**
 *
 * @author David Hickey, TNG Technology Consulting GmbH
 */
@Modification(ConnectorModification.class)
final class SnowflakeDBConnectorNodeSettings2 extends SpecificDBConnectorNodeSettings {

    SnowflakeDBConnectorNodeSettings2() {
        super(Snowflake.DB_TYPE);
    }


    @Layout(ConnectionSection.class)
    @Widget(title = "Full account name", description = """
            Specifies the full name of your account (provided by Snowflake). Note that your full account name
            might include <b>additional</b> segments that identify the region and cloud platform where your account
            is hosted e.g. xy12345.us-east-2.aws, xy12345.us-central1.gcp or xy12345.west-us-2.azure.
            <br />
            The domain <i>.snowflakecomputing.com</i> will be appended automatically to the full account name.
            """)
    @TextInputWidget(patternValidation = IsNotBlankValidation.class)
    String m_accountName;

    @Layout(ConnectionSection.class)
    @Widget(title = "Virtual warehouse", description = """
            Specifies the virtual warehouse to use once connected, or an empty string. The specified warehouse
            should be an existing warehouse for which the specified default role has privileges.
            <br />
            After connecting, the
            <a href="https://docs.snowflake.com/en/sql-reference/sql/use-warehouse.html">USE WAREHOUSE</a> command
            can be executed with the <a href="https://kni.me/n/eh-RgddvYOj-B0uz">DB SQL Executor</a> node to set
            a different database for the session.
            """)
    @TextInputWidget(patternValidation = IsNotBlankValidation.class)
    String m_virtualWarehouseName;

    @Layout(ConnectionSection.class)
    @Widget(title = "Access role", description = """
            Specifies the default access control role to use in the Snowflake session initiated by the driver.
            The specified role should be an existing role that has already been assigned to the specified user
            for the driver. If the specified role has not already been assigned to the user, the role is not
            used when the session is initiated by the driver.
            <br />
            For more information about roles and access control, see
            <a href="https://docs.snowflake.com/en/user-guide/security-access-control.html">\
            Access Control in Snowflake</a>.
            """)
    Optional<String> m_accessRole = Optional.empty();

    @Layout(ConnectionSection.class)
    @Widget(title = "Default database", description = """
            Specifies the default database to use once connected, or an empty string. The specified database
            should be an existing database for which the specified default role has privileges.
            <br />
            After connecting, the
            <a href="https://docs.snowflake.com/en/sql-reference/sql/use-database.html">USE DATABASE</a> command
            can be executed with the <a href="https://kni.me/n/eh-RgddvYOj-B0uz">DB SQL Executor</a> node to set
            a different database for the session.
            """)
    Optional<String> m_defaultDatabase = Optional.empty();

    @Layout(ConnectionSection.class)
    @Widget(title = "Default schema", description = """
            Specifies the default schema to use for the specified database once connected, or an empty string.
            The specified schema should be an existing schema for which the specified default role has privileges.
            <br />
            After connecting, the
            <a href="https://docs.snowflake.com/en/sql-reference/sql/use-schema.html">USE SCHEMA</a> command
            can be executed with the <a href="https://kni.me/n/eh-RgddvYOj-B0uz">DB SQL Executor</a> node to set
            a different schema for the session.
            """)
    Optional<String> m_defaultSchema = Optional.empty();

    @Layout(AuthenticationSection.class)
    @Modification(ChangeAvailableAuthenticationModification.class)
    @Effect(predicate = HasCredentialPortPredicate.class, type = EffectType.HIDE)
    AuthenticationSettings m_authentication =
        new AuthenticationSettings(AuthenticationType.USER_PWD, new Credentials());


    @TextMessage(AuthByCredentialsPortMessage.class)
    @Layout(AuthenticationSection.class)
    Void m_authenticationFromInPortInfo;

    static final class AuthByCredentialsPortMessage extends AuthenticationManagedByPortMessage {

        @Override
        protected boolean hasCredentialPort(final NodeParametersInput input) {
            return SnowflakeDBConnectorNodeSettings2.hasCredentialPort(input);
        }

    }

    static final class HasCredentialPortPredicate extends HasCredentialPort {

        @Override
        protected boolean hasCredentialPort(final NodeParametersInput input) {
            return SnowflakeDBConnectorNodeSettings2.hasCredentialPort(input);
        }

    }

    private static boolean hasCredentialPort(final NodeParametersInput input) {
        return Arrays.stream(input.getInPortTypes())
            .anyMatch(inPortType -> CredentialPortObjectSpec.class.equals(inPortType.getPortObjectSpecClass()));
    }

    static final class ChangeAvailableAuthenticationModification extends AuthenticationTypeModification {
        @Override
        protected Class<? extends EnumChoicesProvider<AuthenticationType>> getAuthenticationTypeChoicesProvider() {
            return AvailableAuthenticationTypesProvider.class;
        }

        @Override
        protected Optional<Class<? extends EffectPredicateProvider>> getRequiresCredentialsEffectProvider() {
            return Optional.of(RequiresCredentialsEffectProvider.class);
        }


        static final class RequiresCredentialsEffectProvider implements EffectPredicateProvider {

            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return and(//
                    i.getPredicate(AuthenticationTypeModification.getDefaultRequiresCredentialsEffectProvider()),
                    not(i.getPredicate(HasCredentialPortPredicate.class))//
                );
            }

        }

        static class AvailableAuthenticationTypesProvider implements EnumChoicesProvider<AuthenticationType> {
            @Override
            public List<AuthenticationType> choices(final NodeParametersInput context) {
                return List.of( //
                    AuthenticationType.NONE, //
                    AuthenticationType.USER, //
                    AuthenticationType.USER_PWD //
                );
            }
        }
    }

    static class ConnectorModification extends ChoicesProviderModification {

        @Override
        protected Class<? extends DBDialectChoicesProvider> getDialectProvider() {
            return DialectProvider.class;
        }

        @Override
        protected Class<? extends DBDriverChoicesProvider> getDriverProvider() {
            return DriverProvider.class;
        }

        static class DialectProvider extends DBDialectChoicesProvider {
            DialectProvider() {
                super(Snowflake.DB_TYPE, false);
            }

            @Override
            public List<StringChoice> computeState(final NodeParametersInput context) {
                return List.of( //
                    new StringChoice(SnowflakeDBSQLDialect.ID, "Snowflake") //
                );
            }
        }

        static class DriverProvider extends DBDriverChoicesProvider {
            DriverProvider() {
                super(Snowflake.DB_TYPE);
            }
        }
    }

    String getDriver() {
        if (m_useLatestDriver) {
            return Optional.ofNullable(DBDriverRegistry.getInstance().getLatestDriver(Snowflake.DB_TYPE)) //
                .map(DBDriverWrapper::getDriverDefinition) //
                .map(DBDriverDefinition::getId) //
                .orElse(m_dbDriver);
        } else {
            return m_dbDriver;
        }
    }

    String getAccountDomain() {
        var attributeValues = m_additionalSettings.m_attributes.entrySet().stream() //
            .filter(entry -> entry.getValue() instanceof Serializable) //
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (Serializable)e.getValue()));

        final AttributeValueRepository valueRepository = AttributeValueRepository.builder()
            .withAttributeCollection(ATTRIBUTES).withMappings(attributeValues).build();
        String domain = valueRepository.get(ATTRIBUTE_ACCOUNT_DOMAIN);
        if (domain == null) {
            domain = SnowflakeAbstractDriverLocator.DEFAULT_ACCOUNT_DOMAIN;
        }
        return domain;
    }

    String getDBUrl() throws InvalidSettingsException {
        return SnowflakeDBConnectorSettings.getDBUrl( //
            getDriver(), //
            m_accountName, //
            getAccountDomain(), //
            m_virtualWarehouseName, //
            m_accessRole.orElse(null), //
            m_defaultDatabase.orElse(null), //
            m_defaultSchema.orElse(null) //
        );
    }
}
