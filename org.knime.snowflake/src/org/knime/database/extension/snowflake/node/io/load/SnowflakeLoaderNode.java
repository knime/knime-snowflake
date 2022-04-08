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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.io.File;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.util.FileUtil;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderFileFormat;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderSettings;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderStageType;
import org.knime.database.extension.snowflake.node.io.load.writer.ConnectedSnowflakeLoaderNodeSettings;
import org.knime.database.model.DBTable;
import org.knime.database.node.component.PreferredHeightPanel;
import org.knime.database.node.io.load.DBLoaderNode2;
import org.knime.database.node.io.load.DBLoaderNode2Factory;
import org.knime.database.node.io.load.ExecutionParameters;
import org.knime.database.node.io.load.impl.fs.util.DBFileWriter;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNode2;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNodeComponents2;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNodeSettings2;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBPortObject;
import org.knime.database.session.DBSession;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocation;
import org.knime.filehandling.core.connections.FSPath;

/**
 * Implementation of the loader node for the Snowflake database.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeLoaderNode extends UnconnectedCsvLoaderNode2
    implements DBLoaderNode2Factory<UnconnectedCsvLoaderNodeComponents2, UnconnectedCsvLoaderNodeSettings2> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SnowflakeLoaderNode.class);


    private static final List<Charset> CHARSETS = unmodifiableList(asList(UTF_8, ISO_8859_1));

    private static Box createBox(final boolean horizontal) {
        final Box box;
        if (horizontal) {
            box = new Box(BoxLayout.X_AXIS);
        } else {
            box = new Box(BoxLayout.Y_AXIS);
        }
        return box;
    }

    private static JPanel createPanel() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        return panel;
    }

    private static void onStageTypeSelectionChange(final SnowflakeLoaderNodeComponents components) {
        final Optional<SnowflakeLoaderStageType> optionalStageType =
            SnowflakeLoaderStageType.optionalValueOf(components.getStageTypeSelectionModel().getStringValue());
        components.getStageNameModel()
            .setEnabled(optionalStageType.isPresent() && optionalStageType.get() == SnowflakeLoaderStageType.INTERNAL);
    }

    private void onFileFormatSelectionChange(final SnowflakeLoaderNodeComponents components) {
        final Optional<SnowflakeLoaderFileFormat> optionalFileFormat =
            SnowflakeLoaderFileFormat.optionalValueOf(components.getFileFormatSelectionModel().getStringValue());
        components.getFileFormatModel()
            .setEnabled(optionalFileFormat.isPresent() && optionalFileFormat.get() == SnowflakeLoaderFileFormat.CSV);

        final SnowflakeLoaderFileFormat fileFormat;
        String compressionFormat = components.getCompressionModel().getStringValue();
        if (optionalFileFormat.isEmpty()) {
            //this can happen only during the first loading of the node set the default file format
            //this is necessary to not overwrite any user entered values for chunk and file size
            fileFormat = SnowflakeLoaderFileFormat.getDefault();
            m_init = false;
            components.getFileFormatSelectionModel().setStringValue(fileFormat.getActionCommand());
        } else {
            fileFormat = optionalFileFormat.get();
        }
        if (!m_init) {
            //the format has truly changed by the user so we need to update the default sizes
            components.getChunkSizeModel().setIntValue(fileFormat.getDefaultChunkSize());
            components.getFileSizeModel().setLongValue(fileFormat.getDefaultFileSize());
            compressionFormat = fileFormat.getDefaultCompressionFormat();
        }

        //always replace the list items to load the possible values but the selected compressionFormat is set before
        //depending on the state
        components.getCompressionComponent().replaceListItems(fileFormat.getCompressionFormats(),
            compressionFormat);
        components.getChunkSizeComponent().setToolTipText(fileFormat.getChunkSizeToolTipText());
        components.getFileSizeComponent().setToolTipText(fileFormat.getFileSizeToolTipText());

        final boolean isCSV =
            optionalFileFormat.isPresent() && optionalFileFormat.get() == SnowflakeLoaderFileFormat.CSV;
        components.getFileFormatModel().setEnabled(isCSV);
        components.getChunkSizeModel().setEnabled(!isCSV);
        components.getFileSizeModel().setEnabled(!isCSV);
    }

    private boolean m_init = false;

    @Override
    public DBLoaderNode2<UnconnectedCsvLoaderNodeComponents2, UnconnectedCsvLoaderNodeSettings2> get() {
        return new SnowflakeLoaderNode();
    }

    @Override
    public void buildDialog(final DialogBuilder builder, final List<DialogComponent> dialogComponents,
        final UnconnectedCsvLoaderNodeComponents2 customComponents) {
        final SnowflakeLoaderNodeComponents snowflakeCustomComponents = (SnowflakeLoaderNodeComponents)customComponents;
        final JPanel optionsPanel = createTargetTablePanel(customComponents);

        optionsPanel.add(fileFormatPanel(snowflakeCustomComponents));

        optionsPanel.add(stagePanel(snowflakeCustomComponents));

        builder.addTab(Integer.MAX_VALUE, "Options", optionsPanel, true);
        builder.addTab(Integer.MAX_VALUE, "Advanced", createAdvancedPanel(snowflakeCustomComponents), true);

        snowflakeCustomComponents.getFileFormatSelectionModel()
            .addChangeListener(event -> onFileFormatSelectionChange(snowflakeCustomComponents));
        snowflakeCustomComponents.getStageTypeSelectionModel()
            .addChangeListener(event -> onStageTypeSelectionChange(snowflakeCustomComponents));
    }

    private static JPanel createAdvancedPanel(final SnowflakeLoaderNodeComponents cc) {
        final JPanel advancedPanel = createPanel();
        final Box advancedBox = createBox(false);
        final JPanel generalPanel = createPanel();
        generalPanel.setBorder(BorderFactory.createTitledBorder(" General Settings "));
        generalPanel.add(cc.getCompressionComponent().getComponentPanel());
        generalPanel.add(cc.getCompressionComponent().getComponentPanel());
        advancedBox.add(generalPanel);
        final JPanel csvPanel = cc.getFileFormatComponent().getComponentPanel();
        csvPanel.setBorder(BorderFactory.createTitledBorder(" CSV Settings "));
        advancedBox.add(csvPanel);
        final JPanel orcParquetPanel = createPanel();
        orcParquetPanel .setBorder(BorderFactory.createTitledBorder(" Parquet Settings "));
        orcParquetPanel.add(cc.getChunkSizeComponent().getComponentPanel());
        orcParquetPanel.add(cc.getFileSizeComponent().getComponentPanel());
        advancedBox.add(orcParquetPanel);
        advancedPanel.add(advancedBox);
        return advancedPanel;
    }

    private static JPanel fileFormatPanel(final SnowflakeLoaderNodeComponents snowflakeCustomComponents) {
        final JPanel fileFormatPanel = new PreferredHeightPanel(new GridBagLayout());
        fileFormatPanel.setBorder(new TitledBorder("File format"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 0;
        fileFormatPanel.add(snowflakeCustomComponents.getFileFormatSelectionComponent().getComponentPanel(), gbc);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        gbc.gridx++;
        fileFormatPanel.add(new JLabel(), gbc);
        return fileFormatPanel;
    }

    private static JPanel stagePanel(final SnowflakeLoaderNodeComponents snowflakeCustomComponents) {
        final JPanel fileFormatPanel = new PreferredHeightPanel(new GridBagLayout());
        fileFormatPanel.setBorder(new TitledBorder("Stage"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 0;
        fileFormatPanel.add(snowflakeCustomComponents.getStageTypeSelectionComponent().getComponentPanel(), gbc);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        gbc.gridx++;
        fileFormatPanel.add(snowflakeCustomComponents.getStageNameComponent().getComponentPanel(), gbc);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        gbc.gridx++;
        fileFormatPanel.add(new JLabel(), gbc);
        return fileFormatPanel;
    }

    @Override
    public DBDataPortObjectSpec configureModel(final PortObjectSpec[] inSpecs, final List<SettingsModel> settingsModels,
        final UnconnectedCsvLoaderNodeSettings2 customSettings) throws InvalidSettingsException {
        final DBPortObject sessionPortObjectSpec = getDBSpec(inSpecs);
        validateColumns(false, createModelConfigurationExecutionMonitor(sessionPortObjectSpec.getDBSession()),
            getDataSpec(inSpecs), sessionPortObjectSpec, customSettings.getTableNameModel().toDBTable());
        return super.configureModel(inSpecs, settingsModels, customSettings);
    }

    @Override
    public SnowflakeLoaderNodeComponents createCustomDialogComponents(final DialogDelegate dialogDelegate) {
        return new SnowflakeLoaderNodeComponents(dialogDelegate, CHARSETS);
    }

    @Override
    public SnowflakeLoaderNodeSettings createCustomModelSettings(final ModelDelegate modelDelegate) {
        return new SnowflakeLoaderNodeSettings(modelDelegate);
    }

    @Override
    public List<DialogComponent> createDialogComponents(final UnconnectedCsvLoaderNodeComponents2 customComponents) {
        //!!!Whenever you change something here you need to adapt the validateModelSettings
        //and loadValidatedModelSettingsFrom methods as well!!!
        final SnowflakeLoaderNodeComponents cc = (SnowflakeLoaderNodeComponents)customComponents;
        return asList(cc.getTableNameComponent(), cc.getFileFormatComponent(), cc.getStageTypeSelectionComponent(),
            cc.getStageNameComponent(), cc.getFileFormatSelectionComponent(), cc.getCompressionComponent(),
            cc.getChunkSizeComponent(), cc.getFileSizeComponent());
    }

    @Override
    public List<SettingsModel> createSettingsModels(final UnconnectedCsvLoaderNodeSettings2 customSettings) {
        //!!!Whenever you change something here you need to adapt the validateModelSettings
        //and loadValidatedModelSettingsFrom methods as well!!!
        final SnowflakeLoaderNodeSettings cs = (SnowflakeLoaderNodeSettings)customSettings;
        return asList(customSettings.getTableNameModel(), customSettings.getFileFormatModel(),
            cs.getStageTypeSelectionModel(), cs.getStageNameModel(), cs.getFileFormatSelectionModel(),
            cs.getCompressionModel(), cs.getChunkSizeModel(), cs.getFileSizeModel());
    }

    @Override
    public void validateModelSettings(final NodeSettingsRO settings, final List<SettingsModel> settingsModels,
        final UnconnectedCsvLoaderNodeSettings2 customSettings) throws InvalidSettingsException {
        //validate the UnconnectedCsvLoaderNodeSettings2
        customSettings.getTableNameModel().validateSettings(settings);
        customSettings.getFileFormatModel().validateSettings(settings);
        //we do not need to pass in the settingsModels list since the elements are created from the customSettings
        ((SnowflakeLoaderNodeSettings)customSettings).validateSettings(settings);
    }

    @Override
    public void loadValidatedModelSettingsFrom(final NodeSettingsRO settings, final List<SettingsModel> settingsModels,
        final UnconnectedCsvLoaderNodeSettings2 customSettings) throws InvalidSettingsException {
        //Load the UnconnectedCsvLoaderNodeSettings2
        customSettings.getTableNameModel().loadSettingsFrom(settings);
        customSettings.getFileFormatModel().loadSettingsFrom(settings);
        //we do not need to pass in the settingsModels list since the elements are created from the customSettings
        ((SnowflakeLoaderNodeSettings)customSettings).loadValidatedModelSettingsFrom(settings);
    }

    @Override
    public DBTable load(final ExecutionParameters<UnconnectedCsvLoaderNodeSettings2> parameters) throws Exception {
        final SnowflakeLoaderNodeSettings customSettings = (SnowflakeLoaderNodeSettings)parameters.getCustomSettings();

        final DBTable table = customSettings.getTableNameModel().toDBTable();
        final SnowflakeLoaderFileFormat fileFormat =
            SnowflakeLoaderFileFormat.optionalValueOf(customSettings.getFileFormatSelectionModel().getStringValue())
                .orElseThrow(() -> new InvalidSettingsException("No file format is selected."));

        final ExecutionMonitor exec = parameters.getExecutionMonitor();
        exec.setMessage("Validating input columns...");
        final DBPortObject dbPortObject = parameters.getDBPortObject();
        final RowInput rowInput = parameters.getRowInput();
        validateColumns(false, exec, rowInput.getDataTableSpec(), dbPortObject, table);
        exec.setProgress(0.1, "Columns successful validated");

        //write file
        final DBSession session = dbPortObject.getDBSession();
        final File tempDir = FileUtil.createTempDir("snowflake-");
        try (DBFileWriter<ConnectedSnowflakeLoaderNodeSettings, SnowflakeLoaderSettings> writer =
            fileFormat.getWriter()) {
            final ConnectedSnowflakeLoaderNodeSettings connectedNodeSettings =
                new ConnectedSnowflakeLoaderNodeSettings(customSettings);
            //set the target folder to a local temporary folder that can be used to upload the data
            final FSLocation tempLocation =  new FSLocation(FSCategory.LOCAL, tempDir.getAbsolutePath());
            connectedNodeSettings.getTargetFolderModel().setLocation(tempLocation);
            final ExecutionParameters<ConnectedSnowflakeLoaderNodeSettings> connectedParameter =
                new ExecutionParameters<>(rowInput, dbPortObject, parameters.getSettingsModels(), connectedNodeSettings,
                    exec);
            exec.setMessage("Writing data files...");
            final FSPath targetFile = writer.write(exec.createSubProgress(0.4), connectedParameter);
            final String targetFileString = targetFile.toAbsolutePath().toString();
            //load file into database table
            LOGGER.debugWithFormat("Target file/directory: \"%s\"", targetFileString);
            // Load the data
            exec.setMessage("Data files successful written");
            exec.checkCanceled();
            session.getAgent(DBLoader.class).load(exec, new DBLoadTableFromFileParameters<>(null, targetFileString,
                table, writer.getLoadParameter(connectedNodeSettings)));
        } finally {
            //cleanup all local temporary files
            try {
                FileUtil.deleteRecursively(tempDir);
            } catch (Exception ex) {
                LOGGER.warn("Error cleaning up temporary directory", ex);
            }
        }
        // Output
        return table;
    }

    @Override
    public void loadDialogSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs,
        final List<DialogComponent> dialogComponents, final UnconnectedCsvLoaderNodeComponents2 customComponents)
        throws NotConfigurableException {
        m_init = true;
        onFileFormatSelectionChange((SnowflakeLoaderNodeComponents)customComponents);
        super.loadDialogSettingsFrom(settings, specs, dialogComponents, customComponents);
        final SnowflakeLoaderNodeComponents snowComponents = (SnowflakeLoaderNodeComponents)customComponents;
        onFileFormatSelectionChange(snowComponents);
        onStageTypeSelectionChange(snowComponents);
        m_init = false;
    }
}
