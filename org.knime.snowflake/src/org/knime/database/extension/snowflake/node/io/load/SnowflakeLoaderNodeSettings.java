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
package org.knime.database.extension.snowflake.node.io.load;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelLongBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderFileFormat;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderStageType;
import org.knime.database.node.io.load.DBLoaderNode2.ModelDelegate;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNodeSettings2;

/**
 * Node model settings for {@link SnowflakeLoaderNode}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeLoaderNodeSettings extends UnconnectedCsvLoaderNodeSettings2 {

    private static final String CFG_FILE_COMPRESSION = "fileCompression";
    private static final String CFG_FILE_CHUNK_SIZE = "withinFileChunkSize";
    private static final String CFG_FILE_SIZE = "fileSize";

    private final SettingsModelString m_fileFormatSelectionModel;
    private final SettingsModelString m_stageTypeSelectionModel;
    private final SettingsModelString m_stageNameModel;

    private final SettingsModelString m_compression;
    private final SettingsModelInteger m_chunkSize;
    private SettingsModelLong m_fileSize;

    /**
     * Constructs a {@link SnowflakeLoaderNodeSettings} object.
     *
     * @param modelDelegate the delegate of the node model to create settings for.
     */
    public SnowflakeLoaderNodeSettings(final ModelDelegate modelDelegate) {
        super(modelDelegate);
        m_fileFormatSelectionModel = createFileFormatSelectionModel();
        m_stageTypeSelectionModel = createStageTypeSelectionModel();
        m_stageNameModel = createStageNameModel();
        m_compression = createCompressionModel();
        m_chunkSize = createChunkSizeModel();
        m_fileSize = createFileSizeModel();
    }

    /**
     * Creates the file format selection settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    static SettingsModelString createFileFormatSelectionModel() {
        return new SettingsModelString("fileFormatSelection", SnowflakeLoaderFileFormat.getDefault().name());
    }

    /**
     * Gets the file format selection settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    public SettingsModelString getFileFormatSelectionModel() {
        return m_fileFormatSelectionModel;
    }

    /**
     * Creates the stage type selection settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    static SettingsModelString createStageTypeSelectionModel() {
        return new SettingsModelString("stageTypeSelection", SnowflakeLoaderStageType.getDefault().name());
    }

    /**
     * Gets the stage type selection settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    public SettingsModelString getStageTypeSelectionModel() {
        return m_stageTypeSelectionModel;
    }

    /**
     * Creates the stage name settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    static SettingsModelString createStageNameModel() {
        return new SettingsModelString("stageName", "");
    }

    /**
     * Gets the stage name settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    public SettingsModelString getStageNameModel() {
        return m_stageNameModel;
    }

    /**
     * Creates the compression model.
     *
     * @return the compression {@link SettingsModelString}
     */
    static SettingsModelString createCompressionModel() {
        return new SettingsModelString(CFG_FILE_COMPRESSION, SnowflakeLoaderFileFormat.GZIP_COMPRESSION);
    }

    /**
     * Returns the compression model.
     *
     * @return the compression
     */
    public SettingsModelString getCompressionModel() {
        return m_compression;
    }


    /**
     * Creates the chunk size model.
     *
     * @return the chunk size {@link SettingsModelInteger}
     */
    static SettingsModelInteger createChunkSizeModel() {
        return new SettingsModelIntegerBounded(CFG_FILE_CHUNK_SIZE, 1024, 1, Integer.MAX_VALUE);
    }

    /**
     * Returns the chunk size model.
     *
     * @return the chunkSize
     */
    public SettingsModelInteger getChunkSizeModel() {
        return m_chunkSize;
    }


    /**
     * Creates the file size model.
     *
     * @return the file size {@link SettingsModelLongBounded}
     */
    static SettingsModelLong createFileSizeModel() {
        return new SettingsModelLongBounded(CFG_FILE_SIZE, 1024, 1, Long.MAX_VALUE);
    }

    /**
     * Returns the file size model.
     *
     * @return the fileSize
     */
    public SettingsModelLong getFileSizeModel() {
        return m_fileSize;
    }

    /**
     * Validates the settings and takes care of backward compatibility.
     *
     * @param settings {@link NodeSettingsRO} to validate
     * @throws InvalidSettingsException if a setting is invalid
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileFormatSelectionModel.validateSettings(settings);
        m_stageTypeSelectionModel.validateSettings(settings);
        m_stageNameModel.validateSettings(settings);
        //the following settings where introduced with 4.5.2
        if (settings.containsKey(CFG_FILE_COMPRESSION)) {
            m_compression.validateSettings(settings);
        }
        if (settings.containsKey(CFG_FILE_CHUNK_SIZE)) {
            m_chunkSize.validateSettings(settings);
        }
        if (settings.containsKey(CFG_FILE_SIZE)) {
            m_fileSize.validateSettings(settings);
        }
    }

    /**
     * Loads the settings and takes care of backward compatibility.
     *
     * @param settings {@link NodeSettingsRO} to load from
     * @throws InvalidSettingsException if a setting is invalid
     */
    void loadValidatedModelSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileFormatSelectionModel.loadSettingsFrom(settings);
        final SnowflakeLoaderFileFormat fileFormat =
                SnowflakeLoaderFileFormat.optionalValueOf(m_fileFormatSelectionModel.getStringValue())
                .orElseThrow(() -> new InvalidSettingsException("No file format is selected."));
        m_stageTypeSelectionModel.loadSettingsFrom(settings);
        m_stageNameModel.loadSettingsFrom(settings);
        //the following settings where introduced with 4.5.2
        if (settings.containsKey(CFG_FILE_COMPRESSION)) {
            m_compression.loadSettingsFrom(settings);
        } else {
            //the old implementation used gzip for CSV explicitly and for Parquet implicitly since for Parquet
            //the auto compress when uploading wasn't disabled
            m_compression.setStringValue(SnowflakeLoaderFileFormat.GZIP_COMPRESSION);
        }
        if (settings.containsKey(CFG_FILE_CHUNK_SIZE)) {
            m_chunkSize.loadSettingsFrom(settings);
        } else {
            m_chunkSize.setIntValue(fileFormat.getDefaultChunkSize());
        }
        if (settings.containsKey(CFG_FILE_SIZE)) {
            m_fileSize.loadSettingsFrom(settings);
        } else {
            m_fileSize.setLongValue(fileFormat.getDefaultFileSize());
        }
    }
}
