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

import static org.apache.commons.lang3.StringUtils.trim;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Collection;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;
import org.knime.core.node.util.StringHistoryPanel;
import org.knime.database.node.connector.server.ServerDBConnectorSettings;
import org.knime.database.node.util.HistoryPanelHelper;

/**
 * A panel for selecting all Snowflake connection related information such as account name, warehouse, role etc.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeConnectionPanel extends JPanel {

    private static final long serialVersionUID = 2319121561117354700L;

    private static final String TITLE = " Connection ";

    private static final String HISTORY_PREFIX = "snowflake_connector_node_";

    private static final String ROLE_HISTORY_ID = HISTORY_PREFIX + "_roleName";

    private static final Collection<String> DEFAULT_ROLES = List.of("ACCOUNTADMIN", "SECURITYADMIN",
        "USERADMIN", "SYSADMIN", "PUBLIC");


    /**
     * An editable drop-down list for entering the account name.
     */
    private final StringHistoryPanel m_accountName;

    /**
     * An editable drop-down list for entering the account name.
     */
    private final StringHistoryPanel m_roleName;

    /**
     * An editable drop-down list for entering the warehouse name.
     */
    private final StringHistoryPanel m_warehouseName;

    /**
     * An editable drop-down list for entering the database name.
     */
    private final StringHistoryPanel m_databaseName;

    /**
     * An editable drop-down list for entering the database name.
     */
    private final StringHistoryPanel m_schemaName;

    /**
     * Creates a server-based database {@linkplain SnowflakeConnectionPanel location panel}.
     *
     */
    SnowflakeConnectionPanel() {
        super(new GridBagLayout());

        m_accountName = new StringHistoryPanel(HISTORY_PREFIX + "_accountName");
        m_warehouseName = new StringHistoryPanel(HISTORY_PREFIX + "_warehouseName");
        m_roleName = new StringHistoryPanel(ROLE_HISTORY_ID);
        m_databaseName = new StringHistoryPanel(HISTORY_PREFIX + "_databaseName");
        m_schemaName = new StringHistoryPanel(HISTORY_PREFIX + "_schemaName");

        presetRoleHistoryIfEmpty();

        setupPanel();
    }

    private static void presetRoleHistoryIfEmpty() {
        final StringHistory history = StringHistory.getInstance(ROLE_HISTORY_ID);
        if (history.getHistory().length == 0) {
            DEFAULT_ROLES.stream().forEach(r -> history.add(r));
        }
    }

    private void setupPanel() {
        setBorder(BorderFactory.createTitledBorder(TITLE));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridy = 0;
        gbc.insets = new Insets(2, 2, 2, 2);
        gbc.anchor = GridBagConstraints.WEST;
        addField("Full account name", m_accountName, gbc);
        addField("Virtual warehouse", m_warehouseName, gbc);
        addField("Default access control role (optional)", m_roleName, gbc);
        addField("Default database (optional)", m_databaseName, gbc);
        addField("Default schema (optional)", m_schemaName, gbc);
    }

    private void addField(final String label, final StringHistoryPanel panel, final GridBagConstraints gbc) {
        gbc.gridx = 0;
        add(new JLabel(label), gbc);
        gbc.gridy++;

        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        add(panel, gbc);
        gbc.gridy++;
    }

    //********************** Save / Load Settings **********************

    /**
     * Updates the settings object with this component's values.
     *
     * @param settings the server-based database connector {@linkplain ServerDBConnectorSettings settings}
     * @throws InvalidSettingsException if settings are invalid
     */
    public void updateSettings(final SnowflakeDBConnectorSettings settings) throws InvalidSettingsException {
        settings.setAccountName(trim(m_accountName.getSelectedString()));
        settings.setWarehouse(m_warehouseName.getSelectedString());
        settings.setRoleName(trim(m_roleName.getSelectedString()));
        settings.setDatabaseName(trim(m_databaseName.getSelectedString()));
        settings.setSchemaName(trim(m_schemaName.getSelectedString()));
    }

    /**
     * Updates this component with the values from the settings object.
     *
     * @param settings the server-based database connector {@linkplain ServerDBConnectorSettings settings}
     * @param specs the incoming port specifications
     * @throws NotConfigurableException if the dialog should not open because necessary information is missing
     */
    public void updateComponent(final SnowflakeDBConnectorSettings settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {
        HistoryPanelHelper.setValueToStringHistoryPanel(m_accountName, settings.getAccountName());
        HistoryPanelHelper.setValueToStringHistoryPanel(m_warehouseName, settings.getWarehouseName());
        HistoryPanelHelper.setValueToStringHistoryPanel(m_roleName, settings.getRoleName());
        HistoryPanelHelper.setValueToStringHistoryPanel(m_databaseName, settings.getDatabaseName());
        HistoryPanelHelper.setValueToStringHistoryPanel(m_schemaName, settings.getSchemaName());
    }
}
