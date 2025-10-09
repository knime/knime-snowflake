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

import java.io.IOException;
import java.util.Optional;

import org.apache.xmlbeans.XmlException;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.database.port.DBSessionPortObject;
import org.xml.sax.SAXException;

/**
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 * @since 5.8
 */
@SuppressWarnings("restriction")
public class SnowflakeDBConnectorNodeFactory2 extends ConfigurableNodeFactory<SnowflakeDBConnectorNodeModel2>
    implements NodeDialogFactory {

    private static final String INPUT_PORT = "Credential (JWT)";

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder() //
        .name("Microsoft SQL Server Connector (labs)") //
        .icon("mssql_server_connector.png") //
        .shortDescription("Create a database connection to Microsoft SQL Server.") //
        .fullDescription("""
                This node creates a connection to a Microsoft SQL Server, an Azure SQL Database or Azure Synapse \
                SQL pool via its JDBC driver. You need to provide the server's hostname (or IP address), the port, \
                and a database name. Login credentials can either be provided directly in the configuration, via \
                credential variables or via the dynamic input port.
                """) //
        .modelSettingsClass(SnowflakeDBConnectorNodeSettings.class) //
        .addExternalResource("https://docs.knime.com/latest/db_extension_guide/index.html#register_jdbc",
            "Database documentation")
        .nodeType(NodeType.Source) //
        .addInputPort(INPUT_PORT, CredentialPortObject.TYPE, """
                When available the node will set the <i>accessToken</i> JDBC parameter automatically based on the \
                information from the connected Microsoft Authenticator node. Depending on your database setup you \
                might need to specify additional JDBC parameters which are described in the Microsoft documentation.
                """, true)
        .addOutputPort("DB Connection", DBSessionPortObject.TYPE, "Microsoft SQL Server DB Connection.") //
        .keywords("database", "db", "connection", "session", "create", "mssql") //
        .build();

    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        return WebUINodeFactory.createNodeDescription(CONFIGURATION);
    }

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final var b = new PortsConfigurationBuilder();
        b.addFixedOutputPortGroup("DB Connection", DBSessionPortObject.TYPE);
        b.addOptionalInputPortGroup(INPUT_PORT, CredentialPortObject.TYPE);
        return Optional.of(b);
    }

    @Override
    protected SnowflakeDBConnectorNodeModel2 createNodeModel(final NodeCreationConfiguration creationConfig) {
        return new SnowflakeDBConnectorNodeModel2(creationConfig.getPortConfig().orElseThrow());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<SnowflakeDBConnectorNodeModel2> createNodeView(final int viewIndex,
        final SnowflakeDBConnectorNodeModel2 nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, SnowflakeDBConnectorNodeSettings.class);
    }
}
