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

import static org.knime.database.extension.snowflake.node.io.load.SnowflakeLoaderNodeSettings.createChunkSizeModel;
import static org.knime.database.extension.snowflake.node.io.load.SnowflakeLoaderNodeSettings.createCompressionModel;
import static org.knime.database.extension.snowflake.node.io.load.SnowflakeLoaderNodeSettings.createFileFormatSelectionModel;
import static org.knime.database.extension.snowflake.node.io.load.SnowflakeLoaderNodeSettings.createFileSizeModel;
import static org.knime.database.extension.snowflake.node.io.load.SnowflakeLoaderNodeSettings.createStageNameModel;
import static org.knime.database.extension.snowflake.node.io.load.SnowflakeLoaderNodeSettings.createStageTypeSelectionModel;

import java.nio.charset.Charset;
import java.util.List;

import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderFileFormat;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderStageType;
import org.knime.database.node.io.load.DBLoaderNode2.DialogDelegate;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNodeComponents2;

/**
 * Node dialog components and corresponding settings for {@link SnowflakeLoaderNode}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeLoaderNodeComponents extends UnconnectedCsvLoaderNodeComponents2 {

    private final DialogComponentButtonGroup m_fileFormatSelectionComponent;
    private final SettingsModelString m_fileFormatSelectionModel;

    private final DialogComponentButtonGroup m_stageTypeSelectionComponent;
    private final SettingsModelString m_stageTypeSelectionModel;

    private final DialogComponentString m_stageNameComponent;
    private final SettingsModelString m_stageNameModel;

    private final DialogComponentStringSelection m_compressionComponent;
    private final SettingsModelString m_compressionModel;

    private final DialogComponentNumber m_chunkSizeComponent;
    private final SettingsModelInteger m_chunkSizeModel;

    private final DialogComponentNumber m_fileSizeComponent;
    private final SettingsModelLong m_fileSizeModel;

    /**
     * Constructs a {@link SnowflakeLoaderNodeComponents} object.
     *
     * @param dialogDelegate the delegate of the node dialog to create components for.
     */
    public SnowflakeLoaderNodeComponents(final DialogDelegate dialogDelegate) {
        super(dialogDelegate);
        m_fileFormatSelectionModel = createFileFormatSelectionModel();
        m_fileFormatSelectionComponent = createFileFormatSelectionComponent(m_fileFormatSelectionModel);

        m_stageTypeSelectionModel = createStageTypeSelectionModel();
        m_stageTypeSelectionComponent = createStageTypeSelectionComponent(m_stageTypeSelectionModel);

        m_stageNameModel = createStageNameModel();
        m_stageNameComponent = createStageNameComponent(m_stageNameModel);

        m_compressionModel = createCompressionModel();
        m_compressionComponent = createCompressionComponent(m_compressionModel);

        m_chunkSizeModel = createChunkSizeModel();
        m_chunkSizeComponent = createChunkSizeComponent(m_chunkSizeModel);

        m_fileSizeModel = createFileSizeModel();
        m_fileSizeComponent = createFileSizeComponent(m_fileSizeModel);
    }

    /**
     * Constructs a {@link SnowflakeLoaderNodeComponents} object.
     *
     * @param dialogDelegate the delegate of the node dialog to create components for.
     * @param charsets the allowed character set options.
     * @throws NullPointerException if {@code charsets} or any of its elements is {@code null}.
     */
    public SnowflakeLoaderNodeComponents(final DialogDelegate dialogDelegate, final List<Charset> charsets) {
        super(dialogDelegate, charsets);
        m_fileFormatSelectionModel = createFileFormatSelectionModel();
        m_fileFormatSelectionComponent = createFileFormatSelectionComponent(m_fileFormatSelectionModel);

        m_stageTypeSelectionModel = createStageTypeSelectionModel();
        m_stageTypeSelectionComponent = createStageTypeSelectionComponent(m_stageTypeSelectionModel);

        m_stageNameModel = createStageNameModel();
        m_stageNameComponent = createStageNameComponent(m_stageNameModel);

        m_compressionModel = createCompressionModel();
        m_compressionComponent = createCompressionComponent(m_compressionModel);

        m_chunkSizeModel = createChunkSizeModel();
        m_chunkSizeComponent = createChunkSizeComponent(m_chunkSizeModel);

        m_fileSizeModel = createFileSizeModel();
        m_fileSizeComponent = createFileSizeComponent(m_fileSizeModel);
    }

    /**
     * Creates the file format selection component.
     *
     * @param fileFormatSelectionModel the already created file format selection settings model.
     * @return a {@link DialogComponentButtonGroup} object.
     */
    protected DialogComponentButtonGroup
        createFileFormatSelectionComponent(final SettingsModelString fileFormatSelectionModel) {
        return new DialogComponentButtonGroup(fileFormatSelectionModel, null, true,
            SnowflakeLoaderFileFormat.values());
    }

    /**
     * Gets the file format selection component.
     *
     * @return a {@link DialogComponentButtonGroup} object.
     */
    public DialogComponentButtonGroup getFileFormatSelectionComponent() {
        return m_fileFormatSelectionComponent;
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
     * Creates the stage type selection component.
     *
     * @param stageTypeSelectionModel the already created stage type selection settings model.
     * @return a {@link DialogComponentButtonGroup} object.
     */
    protected DialogComponentButtonGroup
        createStageTypeSelectionComponent(final SettingsModelString stageTypeSelectionModel) {
        return new DialogComponentButtonGroup(stageTypeSelectionModel, null, true,
            SnowflakeLoaderStageType.values());
    }

    /**
     * Gets the stage type selection component.
     *
     * @return a {@link DialogComponentButtonGroup} object.
     */
    public DialogComponentButtonGroup getStageTypeSelectionComponent() {
        return m_stageTypeSelectionComponent;
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
     * Creates the stage name component.
     *
     * @param stageNameModel the already created stage name settings model.
     * @return a {@link DialogComponentButtonGroup} object.
     */
    protected DialogComponentString createStageNameComponent(final SettingsModelString stageNameModel) {
        return new DialogComponentString(stageNameModel, "Internal stage name: ");
    }

    private static DialogComponentStringSelection
        createCompressionComponent(final SettingsModelString compressionModel) {
        return new DialogComponentStringSelection(compressionModel, "Compression method: ",
            SnowflakeLoaderFileFormat.getDefault().getCompressionFormats());
    }

    private static DialogComponentNumber createChunkSizeComponent(final SettingsModelInteger chunkSizeModel) {
        return new DialogComponentNumber(chunkSizeModel, "Within file chunk size: ", 1024, 10);
    }

    private static DialogComponentNumber createFileSizeComponent(final SettingsModelLong fileSizeModel) {
        return new DialogComponentNumber(fileSizeModel, "File size: ", 1024, 15);
    }

    /**
     * Gets the stage name component.
     *
     * @return a {@link DialogComponentButtonGroup} object.
     */
    public DialogComponentString getStageNameComponent() {
        return m_stageNameComponent;
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
     * Returns the compression component.
     *
     * @return the compressionComponent
     */
    public DialogComponentStringSelection getCompressionComponent() {
        return m_compressionComponent;
    }

    /**
     * Returns the compression model.
     *
     * @return the compressionModel
     */
    public SettingsModelString getCompressionModel() {
        return m_compressionModel;
    }

    /**
     * Returns the chunk size component.
     *
     * @return the chunkSizeComponent
     */
    public DialogComponentNumber getChunkSizeComponent() {
        return m_chunkSizeComponent;
    }

    /**
     * Returns the chunk isze model.
     *
     * @return the chunkSizeModel
     */
    public SettingsModelInteger getChunkSizeModel() {
        return m_chunkSizeModel;
    }

    /**
     * Returns the file size component.
     *
     * @return the fileSizeComponent
     */
    public DialogComponentNumber getFileSizeComponent() {
        return m_fileSizeComponent;
    }

    /**
     * Returns the file size model.
     *
     * @return the fileSizeModel
     */
    public SettingsModelLong getFileSizeModel() {
        return m_fileSizeModel;
    }
}
