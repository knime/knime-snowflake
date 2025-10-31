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
 *   Aug 12, 2025 (david): created
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
 * @author david
 */
public class SnowflakeDBConnectorNodeFactory2 extends ConfigurableNodeFactory<SnowflakeDBConnectorNodeModel2>
    implements NodeDialogFactory {

    private static final String IN_PORT_NAME = "Microsoft Authentication";

    private static final String IN_PORT_DESC =
        """
                <p>
                  This input port allows you to connect a credential port with an
                  <a
                    href="https://docs.snowflake.com/en/user-guide/oauth-intro"
                  >
                    OAuth access token
                  </a>
                  for authentication.
                </p>
                <p>
                  To use
                  <a
                    href="https://docs.snowflake.com/en/user-guide/oauth-snowflake-overview"
                  >
                    Snowflake OAuth
                  </a>
                  you can use the
                  <a
                    href="https://hub.knime.com/n/a5e7aaUxN7A9akSp"
                  >
                    OAuth2 Authenticator
                  </a>
                  node.
                </p>
                <p>
                  For
                  <a
                    href="https://docs.snowflake.com/en/user-guide/oauth-ext-overview"
                  >
                    External OAuth
                  </a>
                  the predecessor node depends on the configured identity provider e.g. for
                  Microsoft Entra ID, you can attach the
                  <a
                    href="https://hub.knime.com/n/f__YQsR0VpxoU5I3"
                  >
                    Microsoft Authenticator
                  </a>
                  node. In the Authenticator node choose one of the OAuth2 authentication
                  types and enter the custom scope you want to use. The scope is specific to
                  the setup of your Azure Entra ID (formerly Azure Active Directory). For
                  further details on how to create a scope or where to find it see the
                  <a
                    href="https://docs.snowflake.com/en/user-guide/oauth-azure.html#step-1-configure-the-oauth-resource-in-azure-ad"
                  >
                    Snowflake documentation.
                  </a>
                </p>
                <p>
                  When available the node will set the
                  <a
                    href="https://docs.snowflake.com/en/user-guide/jdbc-parameters.html#authenticator"
                  >
                    <i>authenticator</i>
                  </a>
                  as well as
                  <a
                    href="https://docs.snowflake.com/en/user-guide/jdbc-parameters.html#token"
                  >
                    <i>token</i>
                  </a>
                  JDBC parameter automatically based on the information from the connected
                  Authenticator node. Depending on your database setup you might need to
                  specify additional JDBC parameters which are described in the
                  <a
                    href="https://docs.snowflake.com/en/user-guide/jdbc-parameters.html#jdbc-driver-connection-parameter-reference"
                  >
                    Snowflake documentation.
                  </a>
                </p>
                <p>
                  For further details on how to configure the connector node for the
                  different supported authentication methods see the
                  <a
                    href="https://docs.knime.com/latest/snowflake_extension_guide/index.html#authentication"
                  >
                    Authentication
                  </a>
                  section of the
                  <a
                    href="https://docs.knime.com/latest/snowflake_extension_guide/index.html"
                  >
                    KNIME Snowflake Extension Guide.
                  </a>
                </p>
                """;

    static final WebUINodeConfiguration CONFIG = WebUINodeConfiguration.builder() //
        .name("Snowflake DB Connector") //
        .icon("snowflake_connector.png") //
        .shortDescription("Create a database connection to Snowflake.") //
        .fullDescription(
            """
                    This node creates a connection to a Snowflake database using the selected Snowflake JDBC driver.
                    <p>
                    To get started with Snowflake in KNIME have a look at the
                    <a href="https://hub.knime.com/knime/collections/KNIME%20for%20Snowflake%20Users~1sIkhkwhAvlptfBj">
                    KNIME for Snowflake Users collection</a> that provides you with links to important KNIME nodes,
                    example workflows and further resources such as the
                    <a href="https://docs.knime.com/latest/snowflake_extension_guide/index.html">
                    KNIME Snowflake Extension Guide.</a>
                     </p><p>
                     For further information about Snowflake in general see the
                    <a href="https://docs.snowflake.com/en/">Snowflake documentation.</a>
                    </p><p>
                    This node uses the selected driver's
                    <a href="https://docs.knime.com/latest/db_extension_guide/index.html#url_template">
                    JDBC URL template</a> to create the concrete database URL. Field validation in the dialog
                    depends on whether the (included)
                    tokens referencing them are mandatory or optional in the template.
                    </p>
                    """) //
        .modelSettingsClass(SnowflakeDBConnectorNodeSettings2.class) //
        .nodeType(NodeType.Source) //
        .addInputPort(IN_PORT_NAME, CredentialPortObject.TYPE, IN_PORT_DESC, true) //
        .addOutputPort("DB Connection", DBSessionPortObject.TYPE, "Snowflake DB Connection.") //
        .keywords("db", "database") //
        .build();

    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        return WebUINodeFactory.createNodeDescription(CONFIG);
    }

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final var b = new PortsConfigurationBuilder();
        b.addFixedOutputPortGroup("DB Connection", DBSessionPortObject.TYPE);
        b.addOptionalInputPortGroup(IN_PORT_NAME, CredentialPortObject.TYPE);
        return Optional.of(b);
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
        return new DefaultNodeDialog(SettingsType.MODEL, SnowflakeDBConnectorNodeSettings2.class);
    }

    @Override
    protected SnowflakeDBConnectorNodeModel2 createNodeModel(final NodeCreationConfiguration creationConfig) {
        return new SnowflakeDBConnectorNodeModel2(creationConfig.getPortConfig().orElseThrow());
    }
}
