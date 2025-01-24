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

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.credentials.base.oauth.api.AccessTokenAccessor;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.connection.UserDBConnectionController;
import org.knime.database.node.connector.AbstractDBConnectorNodeModel;

/**
 * A node model for the <em>Snowflake connector node</em>.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeDBConnectorNodeModel extends AbstractDBConnectorNodeModel<SnowflakeDBConnectorSettings> {

    private final boolean m_hasInputPort;

    /**
     * Constructs an {@link SnowflakeDBConnectorNodeModel} object.
     * @param portsConfiguration {@link PortsConfiguration} with the input port info
     */
    public SnowflakeDBConnectorNodeModel(final PortsConfiguration portsConfiguration) {
        super(new SnowflakeDBConnectorSettings(), portsConfiguration.getInputPorts());
        m_hasInputPort = ArrayUtils.isNotEmpty(portsConfiguration.getInputPorts());
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (m_hasInputPort) {
            final var spec = (CredentialPortObjectSpec) inSpecs[0];
            if (spec.isPresent()) {
                try {
                    spec.toAccessor(AccessTokenAccessor.class);
                } catch (NoSuchCredentialException ex) {
                    throw new InvalidSettingsException(ex.getMessage(), ex);
                }
            }
        }
        return super.configure(inSpecs);
    }

    @Override
    protected DBConnectionController createConnectionController(final NodeSettingsRO internalSettings)
        throws InvalidSettingsException {

        if (m_hasInputPort) {
            return new SnowflakeOAuthDBConnectionController(internalSettings);
        } else {
            return new UserDBConnectionController(internalSettings, getCredentialsProvider());
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    protected DBConnectionController createConnectionController(final List<PortObject> inObjects,
        final SnowflakeDBConnectorSettings sessionSettings, final ExecutionMonitor monitor)
                throws InvalidSettingsException {

        if (m_hasInputPort) {
            try {
                final var spec = (CredentialPortObjectSpec)inObjects.get(0).getSpec();
                final AccessTokenAccessor tokenAccessor = spec.toAccessor(AccessTokenAccessor.class);
                return new SnowflakeOAuthDBConnectionController(tokenAccessor, sessionSettings.getDBUrl());
            } catch (NoSuchCredentialException ex) {
                throw new InvalidSettingsException(ex.getMessage(), ex);
            }
        } else {
            final SettingsModelAuthentication authentication = getSettings().getAuthenticationModel();
            final var credentialsProvider = getCredentialsProvider();
            return new UserDBConnectionController(sessionSettings.getDBUrl(), authentication.getAuthenticationType(),
                authentication.getUserName(credentialsProvider), authentication.getPassword(credentialsProvider),
                authentication.getCredential(), credentialsProvider);
        }
    }
}
