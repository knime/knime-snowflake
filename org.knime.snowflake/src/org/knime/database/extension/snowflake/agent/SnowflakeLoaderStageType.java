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
package org.knime.database.extension.snowflake.agent;

import java.util.Optional;

import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.node.parameters.widget.choices.Label;

/**
 * The stage types supported by the Snowflake data loader node.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public enum SnowflakeLoaderStageType implements ButtonGroupEnumInterface {
        //Stage descriptions: https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage.html
        /**
         * User stage.
         */
        @Label(value = "User stage",
            description = "This stage is a convenient option if your files will only be accessed by a single user, "
                + "but need to be copied into multiple tables.")
        USER("User stage",
            "This stage is a convenient option if your files will only be accessed by a single user, "
                + "but need to be copied into multiple tables.") {
        @Override
        public boolean isDefault() {
            return true;
        }
        },
        /**
         * Table stage.
         */
        @Label(value = "Table stage",
            description = "This stage is a convenient option if your files need to be accessible to multiple "
                + "users and only need to be copied into a single table.")
        TABLE("Table stage",
            "This stage is a convenient option if your files need to be accessible to multiple "
                + "users and only need to be copied into a single table."),
        /**
         * Internal named stage.
         */
        @Label(value = "Internal named stage",
            description = "Named internal stages are optional but recommended when you plan regular "
                + "data loads that could involve multiple users and/or tables.")
        INTERNAL("Internal named stage",
            "Named internal stages are optional but recommended when you plan regular "
                + "data loads that could involve multiple users and/or tables.");

    /**
     * Gets the {@link SnowflakeLoaderStageType} constant with the specified name.
     *
     * @param name the name of the constant.
     * @return {@linkplain Optional optionally} the {@link SnowflakeLoaderStageType} constant with the specified name or
     *         {@linkplain Optional#empty() empty}.
     */
    public static Optional<SnowflakeLoaderStageType> optionalValueOf(final String name) {
        if (name != null) {
            try {
                return Optional.of(valueOf(name));
            } catch (IllegalArgumentException exception) {
                // Ignored.
            }
        }
        return Optional.empty();
    }

    /**
     * Gets the default stage type.
     *
     * @return the default stage type
     */
    public static SnowflakeLoaderStageType getDefault() {
        for (SnowflakeLoaderStageType t : values()) {
            if (t.isDefault()) {
                return t;
            }
        }
        return USER;
    }

    private final String m_text;

    private final String m_toolTip;

    /**
     * Constructor.
     * @param text text
     * @param toolTip tool tip
     */
    SnowflakeLoaderStageType(final String text, final String toolTip) {
        m_text = text;
        m_toolTip = toolTip;
    }

    @Override
    public String getText() {
        return m_text;
    }

    @Override
    public String getActionCommand() {
        return name();
    }

    @Override
    public String getToolTip() {
        return m_toolTip;
    }

    @Override
    public boolean isDefault() {
        return false;
    }

}
