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

import org.knime.database.DBType.DefaultAttributeSupplier;
import org.knime.database.attribute.Attribute;
import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.extension.snowflake.type.Snowflake;
import org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator;
import org.knime.database.util.DerivableProperties;
import org.knime.database.util.DerivableProperties.ValueType;

/**
 * Supplier of all the default database attribute definitions for Snowflake. For Snowflake we use it to ensure
 * that the partner application ID is also used by default for user registered JDBC drivers.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeDefaultAttributeSupplier extends DefaultAttributeSupplier {

    private static final AttributeCollection ATTRIBUTES_CONNECTION;
    /**
     * Attribute that contains the JDBC properties.
     */
    public static final Attribute<DerivableProperties> ATTRIBUTE_JDBC_PROPERTIES;

    static {
        final AttributeCollection.Builder builder = AttributeCollection.builder(
            DBConnectionManagerAttributes.getAttributes());
        //https://docs.snowflake.com/en/user-guide/jdbc-configure.html
        //Snowflake partner use only: Specifies the name of a partner application to connect through JDBC.
        final DerivableProperties jdbcProperties = new DerivableProperties();
        jdbcProperties.setDerivableProperty("application", ValueType.LITERAL, Snowflake.PARTNER_ID);
        ATTRIBUTE_JDBC_PROPERTIES = builder.add(Accessibility.EDITABLE,
            DBConnectionManagerAttributes.ATTRIBUTE_JDBC_PROPERTIES, jdbcProperties);
        // Snowflake needs to be added here for all user registered drivers
        SnowflakeAbstractDriverLocator.addAccountDomainAttribute(builder);
        ATTRIBUTES_CONNECTION = builder.build();
    }


    @Override
    public AttributeCollection getConnectionAttributes() {
        return ATTRIBUTES_CONNECTION;
    }

}
