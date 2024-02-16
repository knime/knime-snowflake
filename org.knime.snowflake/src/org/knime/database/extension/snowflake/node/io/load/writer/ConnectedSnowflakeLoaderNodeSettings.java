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
package org.knime.database.extension.snowflake.node.io.load.writer;

import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.database.extension.snowflake.node.io.load.SnowflakeLoaderNodeSettings;
import org.knime.database.node.component.dbrowser.SettingsModelDBMetadata;
import org.knime.database.node.component.format.file.SettingsModelCsvFileFormat;
import org.knime.database.node.io.load.impl.fs.ConnectedCsvLoaderNodeSettings2;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNodeSettings2;

/**
 * Converts a {@link SnowflakeLoaderNodeSettings} model into a {@link ConnectedCsvLoaderNodeSettings2} model.
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class ConnectedSnowflakeLoaderNodeSettings extends UnconnectedCsvLoaderNodeSettings2 {

    private SnowflakeLoaderNodeSettings m_s;


    /**
     * Constructor.
     *
     * @param s {@link SnowflakeLoaderNodeSettings} to use
     */
    public ConnectedSnowflakeLoaderNodeSettings(final SnowflakeLoaderNodeSettings s) {
        super(s.getModelDelegate());
        m_s = s;
    }

    @Override
    public SettingsModelCsvFileFormat getFileFormatModel() {
        return m_s.getFileFormatModel();
    }

    @Override
    public SettingsModelDBMetadata getTableNameModel() {
        return m_s.getTableNameModel();
    }

    /**
     * Gets the file format selection settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    public SettingsModelString getFileFormatSelectionModel() {
        return m_s.getFileFormatSelectionModel();
    }

    /**
     * Gets the stage type selection settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    public SettingsModelString getStageTypeSelectionModel() {
        return m_s.getStageTypeSelectionModel();
    }

    /**
     * Gets the stage name settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    public SettingsModelString getStageNameModel() {
        return m_s.getStageNameModel();
    }

    /**
     * Returns the compression model.
     *
     * @return the compression
     */
    public SettingsModelString getCompressionModel() {
        return m_s.getCompressionModel();
    }

    /**
     * Returns the chunk size model.
     *
     * @return the chunkSize
     */
    public SettingsModelInteger getChunkSizeModel() {
        return m_s.getChunkSizeModel();
    }

    /**
     * Returns the file size model.
     *
     * @return the fileSize
     */
    public SettingsModelLong getFileSizeModel() {
        return m_s.getFileSizeModel();
    }
}
