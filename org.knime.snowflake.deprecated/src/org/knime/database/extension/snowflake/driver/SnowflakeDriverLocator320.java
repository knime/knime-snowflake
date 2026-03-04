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

package org.knime.database.extension.snowflake.driver;

import static java.util.Arrays.asList;

import java.util.Optional;

import org.knime.core.node.message.Message;
import org.knime.core.node.message.MessageBuilder;
import org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator;

/**
 * This class contains a Snowflake driver definition. The definition will be used by Eclipse extensions API to create a
 * database driver instance.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 *
 */
public class SnowflakeDriverLocator320 extends SnowflakeAbstractDriverLocator {

    private static final Optional<Message> DEPRECATED_MESSAGE = Optional.of(buildDeprecatedMessage());

    private static Message buildDeprecatedMessage() {
        final MessageBuilder builder = Message.builder();
        builder.withSummary("Selected database driver is deprecated.");
        builder.addTextIssue("The selected database driver is no longer supported since it contains a bug that "
            + "causes several database nodes such as the DB Loader and the Snowflake H2O nodes to fail on Windows. "
            + "It will be moved to a separate extension that requires manual installation with one "
            + "of the next releases.");
        builder.addResolutions("Enable the 'Use latest driver version available' option in the node dialog.",
            "Select a new version of the driver in the node dialog.");
        return builder.build().get();
    }

    /**
     * Constructor for {@link SnowflakeDriverLocator320}.
     */
    public SnowflakeDriverLocator320() {
        super("3.20.0", asList("lib/snowflake-jdbc-3.20.0.jar"));
    }

    @Override
    public Optional<Message> getDeprecatedMessage() {
        return DEPRECATED_MESSAGE;
    }

    @Override
    public boolean isDeprecated() {
        return true;
    }
}
