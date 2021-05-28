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

import static java.util.Objects.requireNonNull;

import java.util.Optional;

import org.knime.base.node.io.csvwriter.FileWriterSettings;

/**
 * Additional settings for {@link SnowflakeDBLoader}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeLoaderSettings {

    private final SnowflakeLoaderFileFormat m_fileFormat;

    private final Optional<FileWriterSettings> m_fileWriterSettings;

    private final SnowflakeLoaderStageType m_stageType;

    private final String m_stageName;

    /**
     * Constructs a {@link SnowflakeLoaderSettings} object.
     *
     * @param fileFormat the selected intermediate file format.
     * @param fileWriterSettings the optional file writer settings.
     * @param stageType the {@link SnowflakeLoaderStageType}
     * @param stageName the optional stage name
     */
    public SnowflakeLoaderSettings(final SnowflakeLoaderFileFormat fileFormat,
        final FileWriterSettings fileWriterSettings, final SnowflakeLoaderStageType stageType,
        final String stageName) {
        m_fileFormat = requireNonNull(fileFormat, "fileFormat");
        m_fileWriterSettings = Optional.ofNullable(fileWriterSettings);
        m_stageType = stageType;
        m_stageName = stageName;
    }

    /**
     * Gets the selected intermediate file format.
     *
     * @return a {@link SnowflakeLoaderFileFormat} constant.
     */
    public SnowflakeLoaderFileFormat getFileFormat() {
        return m_fileFormat;
    }

    /**
     * Gets the optional file writer settings.
     *
     * @return {@linkplain Optional optionally} the {@link FileWriterSettings} object or {@linkplain Optional#empty()
     *         empty}.
     */
    public Optional<FileWriterSettings> getFileWriterSettings() {
        return m_fileWriterSettings;
    }

    /**
     * Gets the stage type.
     *
     * @return the SnowflakeLoaderStageType
     */
    public SnowflakeLoaderStageType getStageType() {
        return m_stageType;
    }

    /**
     * Gets the optional stage name.
     *
     * @return the stage name
     */
    public String getStageName() {
        return m_stageName;
    }

}
