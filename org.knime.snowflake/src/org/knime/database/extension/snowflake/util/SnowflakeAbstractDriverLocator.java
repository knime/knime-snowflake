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

package org.knime.database.extension.snowflake.util;

import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_DATABASE;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_SCHEMA;

import java.util.Collection;
import java.util.TimeZone;

import org.knime.core.node.message.Message;
import org.knime.core.node.message.MessageBuilder;
import org.knime.database.DBType;
import org.knime.database.attribute.Attribute;
import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;
import org.knime.database.attribute.AttributeGroup;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.driver.AbstractDriverLocator;
import org.knime.database.driver.DBDriverLocator;
import org.knime.database.extension.snowflake.type.Snowflake;
import org.knime.database.util.DerivableProperties;
import org.knime.database.util.DerivableProperties.ValueType;

/**
 * Abstract class that should be extended by all Snowflake {@link AbstractDriverLocator} implementations.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public abstract class SnowflakeAbstractDriverLocator extends AbstractDriverLocator {

    /**The initial jdbc parameter separator. */
    public static final String ATTR_VAL_JDBC_INITIAL_PARAMETER_SEPARATOR = "&";
    /**
     * Name of the account name variable in the JDBC URL template string.
     * @see #getURLTemplate()
     */
    public static final String VARIABLE_NAME_ACCOUNT_NAME = "account_name";
    /**
     * Name of the account domain (e.g. snowflakecomputing.com) variable in the JDBC URL template string.
     * @see #getURLTemplate()
     */
    public static final String VARIABLE_NAME_ACCOUNT_DOMAIN = "account_domain";
    /**
     * Name of the role variable in the JDBC URL template string.
     * @see #getURLTemplate()
     */
    public static final String VARIABLE_NAME_ROLE = "role";
    /**
     * Name of the warehouse variable in the JDBC URL template string.
     * @see #getURLTemplate()
     */
    public static final String VARIABLE_NAME_WAREHOUSE = "warehouse";
    /**
     * The {@link AttributeCollection} {@linkplain #getAttributes() of} Snowflake drivers.
     */
    public static final AttributeCollection ATTRIBUTES;
    /**
     * Attribute that contains the JDBC properties.
     */
    public static final Attribute<DerivableProperties> ATTRIBUTE_JDBC_PROPERTIES;

    /**Default account domain.*/
    public static final String DEFAULT_ACCOUNT_DOMAIN = "snowflakecomputing.com";

    /**The account domain attribute.*/
    public static final Attribute<String> ATTRIBUTE_ACCOUNT_DOMAIN;

    static {
        final AttributeCollection.Builder builder =
            AttributeCollection.builder(DBConnectionManagerAttributes.getAttributes());
        //https://docs.snowflake.com/en/user-guide/jdbc-configure.html
        //Snowflake partner use only: Specifies the name of a partner application to connect through JDBC.
        final DerivableProperties jdbcProperties = new DerivableProperties();
        jdbcProperties.setDerivableProperty("application", ValueType.LITERAL, Snowflake.PARTNER_ID);
        //we need to set this parameter to prevent problems with the timestamp type (see AP-16726)
        jdbcProperties.setDerivableProperty("TIMEZONE", ValueType.LITERAL, TimeZone.getDefault().getID());
        ATTRIBUTE_JDBC_PROPERTIES = builder.add(Accessibility.EDITABLE,
            DBConnectionManagerAttributes.ATTRIBUTE_JDBC_PROPERTIES, jdbcProperties);
        //change only visibility but keep the default values
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_PARAMETER_TO_URL);
        //the initial parameter separator is already the subsequent separator because we already add
        //parameters to the URL e.g. warehouse, database etc.
        builder.add(Accessibility.HIDDEN,
            DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_INITIAL_PARAMETER_SEPARATOR,
            ATTR_VAL_JDBC_INITIAL_PARAMETER_SEPARATOR);
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_PARAMETER_SEPARATOR);
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_USER_AND_PASSWORD_TO_URL);
        // Snowflake needs to be added here for all built-in drivers
        ATTRIBUTE_ACCOUNT_DOMAIN = addAccountDomainAttribute(builder);
        ATTRIBUTES = builder.build();
    }

    /**
     * Adds the account domain attribute to the given builder.
     *
     * @param builder {@link AttributeCollection.Builder} to add the account domain attribute
     * @return the added {@link Attribute}
     */
    public static Attribute<String> addAccountDomainAttribute(final AttributeCollection.Builder builder) {
        return builder.setGroup(new AttributeGroup("snowflake", "Snowflake")).add(Accessibility.EDITABLE,
            "snowflake.account.domain", DEFAULT_ACCOUNT_DOMAIN, "Account domain",
            "The domain part of your Snowflake account which replaces the &lt;" + VARIABLE_NAME_ACCOUNT_DOMAIN
            + "&gt; JDBC URL template token.");
    }

    /**
     * Helper method to create a standardized Snowflake driver deprecation message.
     *
     * @param deprecationDate the date the driver gets deprecated e.g. February 2025
     * @return a default deprecated message
     */
    protected static Message buildDeprecatedMessage(final String deprecationDate) {
        final MessageBuilder builder = Message.builder();
        builder.withSummary("Selected database driver is deprecated.");
        builder.addTextIssue("As of " + deprecationDate + ", the selected database driver is no longer supported "
            + "by Snowflake. It will be moved to a separate extension that requires manual installation with one "
            + "of the next releases.");
        builder.addResolutions("Enable the 'Use latest driver version available' option in the node dialog.",
            "Select a new version of the driver in the node dialog.");
        return builder.build().get();
    }

    private final String m_version;

    private final Collection<String> m_driverPaths;

    /**
     * Constructor for the driver locator.
     *
     * @param version the version of the driver
     * @param driverPaths the path to the driver jars
     */
    public SnowflakeAbstractDriverLocator(final String version, final Collection<String> driverPaths) {
        super(ATTRIBUTES);
        m_version = version;
        m_driverPaths = driverPaths;
    }

    /**
     * Returns a string representation of the driver version.
     *
     * @return the driver version
     */
    protected String getVersion() {
        return m_version;
    }

    @Override
    public String getDriverId() {
        return DBDriverLocator.createDriverId(getDBType(), getVersion());
    }

    @Override
    public String getDriverName() {
        return DBDriverLocator.createDriverName(getDBType(), getVersion(), isDeprecated());
    }

    @Override
    public String getDriverClassName() {
        return "net.snowflake.client.jdbc.SnowflakeDriver";
    }

    @Override
    public DBType getDBType() {
        return Snowflake.DB_TYPE;
    }

    @Override
    public Collection<String> getDriverPaths() {
        return m_driverPaths;
    }

    @Override
    public String getURLTemplate() {
        //TODO how to handle VPS installations
        return "jdbc:snowflake://<" + VARIABLE_NAME_ACCOUNT_NAME + ">.<" + VARIABLE_NAME_ACCOUNT_DOMAIN + ">/" //
                + "?warehouse=<" + VARIABLE_NAME_WAREHOUSE + ">" //forced line break
                + "&role=[" + VARIABLE_NAME_ROLE + "]" + "&db=[" + VARIABLE_NAME_DATABASE + "]" //forced line break
                + "&schema=[" + VARIABLE_NAME_SCHEMA + "]";
    }

}
