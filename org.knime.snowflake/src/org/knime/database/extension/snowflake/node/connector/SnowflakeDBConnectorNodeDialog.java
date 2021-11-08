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

import static org.knime.database.node.connector.DBConnectorUIHelper.INSETS_PANEL;

import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import javax.swing.Box;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.util.Pair;
import org.knime.database.extension.snowflake.type.Snowflake;
import org.knime.database.node.connector.AbstractDBConnectorNodeDialog;
import org.knime.database.node.connector.ConfigurationPanel;
import org.knime.database.node.connector.DBConnectorUIHelper;
import org.knime.database.node.connector.domain.DBTypeUI;

/**
 * A node dialog for the <em>Snowflake connector node</em>.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeDBConnectorNodeDialog extends AbstractDBConnectorNodeDialog<SnowflakeDBConnectorSettings> {

    private static final DBTypeUI DB_TYPE = new DBTypeUI(Snowflake.DB_TYPE);


    private static final Collection<AuthenticationType> AUTH_TYPES = Arrays.asList(AuthenticationType.NONE,
        AuthenticationType.USER, AuthenticationType.USER_PWD, AuthenticationType.CREDENTIALS);

    private static final Map<AuthenticationType, Pair<String, String>> NAMING_MAP = null;

    private final SnowflakeConnectionPanel m_locationPanel = new SnowflakeConnectionPanel();

    private final SettingsModelAuthentication m_modelAuthentication = getSettings().getAuthenticationModel();

    private final DialogComponentAuthentication m_componentAuthentication;

    private final boolean m_hasInputPort;

    /**
     * Constructs an {@link SnowflakeDBConnectorNodeDialog}.
     * @param portsConfiguration {@link PortsConfiguration} with the input port info
     */
    public SnowflakeDBConnectorNodeDialog(final PortsConfiguration portsConfiguration) {
        super(new SnowflakeDBConnectorSettings(),
            new ConfigurationPanel<>(" Configuration ", () -> new DBTypeUI[]{DB_TYPE},
                DBConnectorUIHelper::getNonDefaultDBDialects, DBConnectorUIHelper::getDBDrivers));
        m_componentAuthentication =
                new DialogComponentAuthentication(m_modelAuthentication, "Authentication", AUTH_TYPES, NAMING_MAP);
        m_hasInputPort = ArrayUtils.isNotEmpty(portsConfiguration.getInputPorts());
        initializeConnectionSettingsPanel();
    }

    /**
     * Initializes the panel of the <em>Connection Settings</em> tab.
     */
    protected void initializeConnectionSettingsPanel() {
        final JPanel panel = getConnectionSettingsPanel();
        final GridBagConstraints gbc = new GridBagConstraints();
        // connection
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.gridy = 0;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = INSETS_PANEL;
        panel.add(getConfigurationPanel(), gbc);
        // location
        gbc.gridy++;
        panel.add(m_locationPanel, gbc);
        // authentication
        gbc.gridy++;
        if (!m_hasInputPort) {
            // authentication
            panel.add(m_componentAuthentication.getComponentPanel(), gbc);
        } else {
            gbc.insets = new Insets(5, 5, 0, 0);
            panel.add(new JLabel("Using information from input connection for authentication"), gbc);
            gbc.insets = INSETS_PANEL;
        }
        //add resizeable box to move components to top
        gbc.gridy++;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.BOTH;
        panel.add(Box.createVerticalBox(), gbc);
    }

    //********************** Save / Load Settings **********************

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        // The authentication data saving is redundant, but it makes sure that the component validation runs.
        m_componentAuthentication.saveSettingsTo(settings);
        super.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        // The authentication data loading is redundant, but the call is required for component initialization.
        m_componentAuthentication.loadSettingsFrom(settings, specs, getCredentialsProvider());
    }

    @Override
    protected void updateComponents(final SnowflakeDBConnectorSettings settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {
        super.updateComponents(settings, specs);
        m_locationPanel.updateComponent(getSettings(), specs);
    }

    @Override
    protected void updateSettings(final SnowflakeDBConnectorSettings settings) throws InvalidSettingsException {
        super.updateSettings(settings);
        m_locationPanel.updateSettings(getSettings());
    }
}
