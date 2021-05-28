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
import static java.nio.file.Files.delete;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.knime.base.filehandling.remote.files.RemoteFileFactory.createRemoteFile;
import static org.knime.core.util.FileUtil.createTempFile;
import static org.knime.database.util.CsvFiles.writeCsv;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.knime.base.node.io.csvwriter.FileWriterSettings;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetTypeMappingService;
import org.knime.bigdata.fileformats.parquet.writer.ParquetKNIMEWriter;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.RowInput;
import org.knime.database.DBTableSpec;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderFileFormat;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderSettings;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderStageType;
import org.knime.database.model.DBColumn;
import org.knime.database.model.DBTable;
import org.knime.database.node.component.PreferredHeightPanel;
import org.knime.database.node.io.load.DBLoaderNode2;
import org.knime.database.node.io.load.DBLoaderNode2Factory;
import org.knime.database.node.io.load.ExecutionParameters;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNode2;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNodeComponents2;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedCsvLoaderNodeSettings2;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBPortObject;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;

/**
 * Implementation of the loader node for the Snowflake database.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeLoaderNode extends UnconnectedCsvLoaderNode2
    implements DBLoaderNode2Factory<UnconnectedCsvLoaderNodeComponents2, UnconnectedCsvLoaderNodeSettings2> {
    private static final Map<SQLType, ParquetType> SNOWFLAKE_TO_PARQUET_TYPE_MAPPING;
    static {
        final Map<SQLType, ParquetType> map = new HashMap<>();
        map.put(JDBCType.BOOLEAN, new ParquetType(PrimitiveTypeName.BOOLEAN));
        map.put(JDBCType.DOUBLE, new ParquetType(PrimitiveTypeName.DOUBLE));
        map.put(JDBCType.BIGINT, new ParquetType(PrimitiveTypeName.INT64));
        map.put(JDBCType.VARCHAR, new ParquetType(PrimitiveTypeName.BINARY, OriginalType.UTF8));
        map.put(JDBCType.DATE, new ParquetType(PrimitiveTypeName.INT32, OriginalType.DATE));
        map.put(JDBCType.TIME, new ParquetType(PrimitiveTypeName.INT32, OriginalType.TIME_MILLIS));
        map.put(JDBCType.TIMESTAMP, new ParquetType(PrimitiveTypeName.INT64, OriginalType.TIMESTAMP_MICROS));
        SNOWFLAKE_TO_PARQUET_TYPE_MAPPING = unmodifiableMap(map);
    }

    private static final List<Charset> CHARSETS = unmodifiableList(asList(UTF_8, ISO_8859_1));

    //report progress only every x rows
    private static final long PROGRESS_THRESHOLD = 1000;

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

    private static void onFileFormatSelectionChange(final SnowflakeLoaderNodeComponents components) {
        final Optional<SnowflakeLoaderFileFormat> optionalFileFormat =
            SnowflakeLoaderFileFormat.optionalValueOf(components.getFileFormatSelectionModel().getStringValue());
        components.getFileFormatModel()
            .setEnabled(optionalFileFormat.isPresent() && optionalFileFormat.get() == SnowflakeLoaderFileFormat.CSV);
    }

    private static void onStageTypeSelectionChange(final SnowflakeLoaderNodeComponents components) {
        final Optional<SnowflakeLoaderStageType> optionalStageType =
            SnowflakeLoaderStageType.optionalValueOf(components.getStageTypeSelectionModel().getStringValue());
        components.getStageNameModel()
            .setEnabled(optionalStageType.isPresent() && optionalStageType.get() == SnowflakeLoaderStageType.INTERNAL);
    }

    private static void writeParquet(final RowInput rowInput, final Path file, final DBTable table,
        final DBSession session, final ExecutionMonitor executionMonitor) throws Exception {
        executionMonitor.checkCanceled();
        final DataTableSpec inputTableSpec = rowInput.getDataTableSpec();
        long i = 0;
        long rowCnt = -1;
        if (rowInput instanceof DataTableRowInput) {
            rowCnt = ((DataTableRowInput)rowInput).getRowCount();
        }
        try (ParquetKNIMEWriter writer = new ParquetKNIMEWriter(createRemoteFile(file.toUri(), null, null),
            inputTableSpec, CompressionCodecName.UNCOMPRESSED.name(), -1,
            createParquetTypeMappingConfiguration(inputTableSpec, table, session, executionMonitor))) {
            for (DataRow row = rowInput.poll(); row != null; row = rowInput.poll()) {
                if (i % PROGRESS_THRESHOLD == 0) {
                    // set the progress
                    executionMonitor.checkCanceled();
                    final long finalI = i;
                    if (rowCnt <= 0) {
                        executionMonitor.setMessage(() -> "Writing row " + finalI);
                    } else {
                        final long finalRowCnt = rowCnt;
                        executionMonitor.setProgress(i / (double)rowCnt,
                            () -> "Writing row " + finalI + " of " + finalRowCnt);
                    }
                }
                writer.writeRow(row);
                i++;
            }
        }
        executionMonitor.setProgress(1, "Temporary Parquet file has been written.");
    }

    private static DataTypeMappingConfiguration<ParquetType> createParquetTypeMappingConfiguration(
        final DataTableSpec inputTableSpec, final DBTable targetTable, final DBSession session,
        final ExecutionMonitor executionMonitor) throws CanceledExecutionException, SQLException {
        final DBTableSpec targetTableSpec =
            session.getAgent(DBMetadataReader.class).getDBTableSpec(executionMonitor, targetTable);
        final ParquetTypeMappingService typeMappingService = ParquetTypeMappingService.getInstance();
        final DataTypeMappingConfiguration<ParquetType> result =
            typeMappingService.createMappingConfiguration(KNIME_TO_EXTERNAL);
        int columnIndex = 0;
        for (final DBColumn column : targetTableSpec) {
            final SQLType sqlType = column.getColumnType();
            final ParquetType parquetType = SNOWFLAKE_TO_PARQUET_TYPE_MAPPING.get(sqlType);
            if (parquetType == null) {
                throw new SQLException(
                    "Parquet type could not be found for the SQL type: " + sqlType);
            }
            final DataType inputColumnType = inputTableSpec.getColumnSpec(columnIndex++).getType();
            result.addRule(inputColumnType,
                typeMappingService.getConsumptionPathsFor(inputColumnType).stream()
                    .filter(path -> path.getConsumerFactory().getDestinationType().equals(parquetType)).findFirst()
                    .orElseThrow(() -> new SQLException("Consumption path could not be found from " + inputColumnType
                        + " through " + parquetType + " to " + sqlType + '.')));
        }
        return result;
    }

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
        final JPanel advancedPanel = createPanel();
        final Box advancedBox = createBox(false);
        advancedPanel.add(advancedBox);
        advancedBox.add(snowflakeCustomComponents.getFileFormatComponent().getComponentPanel());
        builder.addTab(Integer.MAX_VALUE, "Advanced", advancedPanel, true);

        snowflakeCustomComponents.getFileFormatSelectionModel()
            .addChangeListener(event -> onFileFormatSelectionChange(snowflakeCustomComponents));
        snowflakeCustomComponents.getStageTypeSelectionModel()
            .addChangeListener(event -> onStageTypeSelectionChange(snowflakeCustomComponents));
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
        return asList(customComponents.getTableNameComponent(),
            ((SnowflakeLoaderNodeComponents)customComponents).getStageTypeSelectionComponent(),
            ((SnowflakeLoaderNodeComponents)customComponents).getStageNameComponent(),
            ((SnowflakeLoaderNodeComponents)customComponents).getFileFormatSelectionComponent(),
            customComponents.getFileFormatComponent());
    }

    @Override
    public List<SettingsModel> createSettingsModels(final UnconnectedCsvLoaderNodeSettings2 customSettings) {
        return asList(customSettings.getTableNameModel(),
            ((SnowflakeLoaderNodeSettings)customSettings).getStageTypeSelectionModel(),
            ((SnowflakeLoaderNodeSettings)customSettings).getStageNameModel(),
            ((SnowflakeLoaderNodeSettings)customSettings).getFileFormatSelectionModel(),
            customSettings.getFileFormatModel());
    }

    @Override
    public DBTable load(final ExecutionParameters<UnconnectedCsvLoaderNodeSettings2> parameters) throws Exception {
        final SnowflakeLoaderNodeSettings customSettings = (SnowflakeLoaderNodeSettings)parameters.getCustomSettings();

        final DBTable table = customSettings.getTableNameModel().toDBTable();
        final SnowflakeLoaderFileFormat fileFormat =
            SnowflakeLoaderFileFormat.optionalValueOf(customSettings.getFileFormatSelectionModel().getStringValue())
                .orElseThrow(() -> new InvalidSettingsException("No file format is selected."));

        final SnowflakeLoaderStageType stageType =
            SnowflakeLoaderStageType.optionalValueOf(customSettings.getStageTypeSelectionModel().getStringValue())
                .orElseThrow(() -> new InvalidSettingsException("No stage type is selected."));
        final String stageName = customSettings.getStageNameModel().getStringValue();

        final ExecutionMonitor executionContext = parameters.getExecutionMonitor();
        final DBPortObject dbPortObject = parameters.getDBPortObject();
        validateColumns(false, executionContext, parameters.getRowInput().getDataTableSpec(), dbPortObject, table);
        executionContext.setProgress(0.1);
        final DBSession session = dbPortObject.getDBSession();
        final String fileExtension =
            fileFormat.getFileExtension() + (SnowflakeLoaderFileFormat.CSV == fileFormat ? ".gz" : "");
        // Create and write to the temporary file
        final Path temporaryFile = createTempFile("knime2db", fileExtension).toPath();
        try (AutoCloseable temporaryFileDeleter = () -> delete(temporaryFile)) {
            executionContext.setMessage("Writing temporary file...");
            final ExecutionMonitor subExec = executionContext.createSubProgress(0.7);
            final SnowflakeLoaderSettings additionalSettings;
            switch (fileFormat) {
                case CSV:
                    final FileWriterSettings fileWriterSettings =
                        customSettings.getFileFormatModel().getFileWriterSettings();
                    writeCsv(parameters.getRowInput(), temporaryFile, fileWriterSettings, subExec, true);
                    additionalSettings =
                        new SnowflakeLoaderSettings(fileFormat, fileWriterSettings, stageType, stageName);
                    break;
                case PARQUET:
                    writeParquet(parameters.getRowInput(), temporaryFile, table, session, subExec);
                    additionalSettings = new SnowflakeLoaderSettings(fileFormat, null, stageType, stageName);
                    break;
                default:
                    throw new InvalidSettingsException("Unknown file format: " + fileFormat);
            }
            subExec.setProgress(1.0);
            // Load the data
            executionContext.setMessage("Loading file into Snowflake");
            session.getAgent(DBLoader.class).load(executionContext,
                new DBLoadTableFromFileParameters<>(null, temporaryFile.toString(), table, additionalSettings));
        }
        // Output
        return table;
    }

    @Override
    public void loadDialogSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs,
        final List<DialogComponent> dialogComponents, final UnconnectedCsvLoaderNodeComponents2 customComponents)
        throws NotConfigurableException {
        super.loadDialogSettingsFrom(settings, specs, dialogComponents, customComponents);
        onFileFormatSelectionChange((SnowflakeLoaderNodeComponents)customComponents);
        onStageTypeSelectionChange((SnowflakeLoaderNodeComponents)customComponents);
    }
}
