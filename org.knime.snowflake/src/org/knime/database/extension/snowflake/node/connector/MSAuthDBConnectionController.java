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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.database.VariableContext;
import org.knime.database.attribute.AttributeValueRepository;
import org.knime.database.connection.UrlDBConnectionController;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredential;
import org.knime.ext.microsoft.authentication.port.oauth2.OAuth2Credential;

/**
 * {@link UrlDBConnectionController} that uses a given {@link MicrosoftCredential} as login information.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 * @since 4.5
 */
public class MSAuthDBConnectionController extends UrlDBConnectionController {

    private static final String JDBC_PROPERTY_AUTHENTICATOR_OAUTH = "oauth";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(MSAuthDBConnectionController.class);

    private static final String JDBC_PROPERTY_AUTHENTICATOR = "authenticator";

    private static final String JDBC_PROPERTY_TOKEN = "token";

    private final MicrosoftCredential m_credential;

    /**
     * Constructs a {@link MSAuthDBConnectionController} object.
     *
     * @param internalSettings {@link NodeSettingsRO} to read from
     * @throws InvalidSettingsException if the credential can not be loaded
     */
    public MSAuthDBConnectionController(final NodeSettingsRO internalSettings) throws InvalidSettingsException {
        super(internalSettings);
        throw new InvalidSettingsException(
            "Restoring of database connection not supported. Please re-execute the node.");
    }

    /**
     * Constructs a {@link MSAuthDBConnectionController} object.
     *
     * @param credential {@link MicrosoftCredential} to use for authentication
     * @param jdbcUrl the database connection URL as a {@link String}.
     * @throws NullPointerException if {@code jdbcUrl} is {@code null}.
     */
    public MSAuthDBConnectionController(final MicrosoftCredential credential, final String jdbcUrl) {
        super(jdbcUrl);
        m_credential = credential;
    }

    @Override
    public void saveInternalsTo(final NodeSettingsWO settings) {
        super.saveInternalsTo(settings);
        //nothing to save token can not be restored
    }

    @Override
    protected Properties prepareJdbcProperties(final AttributeValueRepository attributeValues,
        final VariableContext variableContext, final ExecutionMonitor monitor)
        throws CanceledExecutionException, SQLException {
        final Properties jdbcProperties =
            getDerivableJdbcProperties(attributeValues).getDerivedProperties(variableContext);
        final String userEnteredToken = jdbcProperties.getProperty(JDBC_PROPERTY_TOKEN);
        if (StringUtils.isNoneBlank(userEnteredToken)) {
            LOGGER.warn("User entered " + JDBC_PROPERTY_TOKEN + " parameter will be overwritten with the "
                + "input connection information.");
        }
        try {
            final String token;
            switch (m_credential.getType()) {
                case OAUTH2_ACCESS_TOKEN:
                    final OAuth2Credential oauth2Credential = (OAuth2Credential)m_credential;
                    token = oauth2Credential.getAccessToken().getToken();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported credential type " + m_credential.getType());
            }
            jdbcProperties.setProperty(JDBC_PROPERTY_TOKEN, token);

            final String userEnteredAuthenticator = jdbcProperties.getProperty(JDBC_PROPERTY_AUTHENTICATOR);
            if (StringUtils.isNoneBlank(userEnteredAuthenticator)
                    && !JDBC_PROPERTY_AUTHENTICATOR_OAUTH.equalsIgnoreCase(userEnteredAuthenticator)) {
                LOGGER.warn("User entered " + JDBC_PROPERTY_AUTHENTICATOR + " parameter will be overwritten with "
                    + JDBC_PROPERTY_AUTHENTICATOR_OAUTH);
            }
            jdbcProperties.setProperty(JDBC_PROPERTY_AUTHENTICATOR, JDBC_PROPERTY_AUTHENTICATOR_OAUTH);
            monitor.checkCanceled();
            LOGGER.debug("Using Microsoft access token to establish database connection");
            return jdbcProperties;
        } catch (IOException ex) {
            throw new SQLException("Error getting Micorosft authentication access token: " + ex.getMessage(), ex);
        }
    }
}
