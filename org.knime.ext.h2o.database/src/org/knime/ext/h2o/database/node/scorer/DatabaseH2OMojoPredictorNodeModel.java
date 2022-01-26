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

package org.knime.ext.h2o.database.node.scorer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.core.runtime.URIUtil;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.database.DBDataObject;
import org.knime.database.SQLQuery;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.node.DBNodeModel;
import org.knime.database.node.util.DBNodeModelHelper;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.ext.h2o.mojo.H2OMojoPortObject;
import org.knime.ext.h2o.mojo.H2OMojoPortObjectSpec;
import org.knime.ext.h2o.mojo.nodes.scorer.H2OGeneralMojoPredictorConfig;
import org.knime.ext.h2o.mojo.nodes.scorer.H2OMojoPredictorUtils;
import org.knime.filehandling.core.connections.DefaultFSConnectionFactory;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.noconfig.NoConfigURIExporterFactory;
import org.knime.snowflake.h2o.companion.udf.MojoPredictor;

/**
 * Basic node model for all Snowflake MOJO predictor nodes.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public abstract class DatabaseH2OMojoPredictorNodeModel extends DBNodeModel {

    /** The name of the name column in the staged files table returned by the Snowflake LS command. */
    private static final String COL_STAGED_FILE_NAME = "name";

    /** The config of the node. */
    private H2OGeneralMojoPredictorConfig m_config = createConfig();

    /** {@link PortObjectSpec} of the {@link H2OMojoPortObject}. */
    private H2OMojoPortObjectSpec m_mojoSpec;

    /**
     * Constructor.
     */
    protected DatabaseH2OMojoPredictorNodeModel() {
        super(new PortType[]{H2OMojoPortObject.TYPE, DBDataPortObject.TYPE}, new PortType[]{DBDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_mojoSpec = (H2OMojoPortObjectSpec)inSpecs[0];

        final DBDataPortObjectSpec dbPortSpec = DBNodeModelHelper.asDBDataPortObjectSpec(inSpecs[1]);
        final DataTableSpec tableSpec = dbPortSpec.getDataTableSpec();

        // Check input model
        H2OMojoPredictorUtils.validateModelCategory(m_mojoSpec, m_config);
        // Check input table
        validateInputTable(tableSpec);

        validateInternal(tableSpec, m_config);

        return new PortObjectSpec[]{null};
    }

    /**
     * Checks the columns of the input table for correct names and types. This function may need to be overridden.
     *
     * @param tableSpec input {@link DataTableSpec}
     * @throws InvalidSettingsException if the name or type of an input column is missing or does not fit
     */
    protected void validateInputTable(final DataTableSpec tableSpec) throws InvalidSettingsException {
        final List<String> warningMsgs =
            H2OMojoPredictorUtils.validateInputTable(tableSpec, m_mojoSpec, m_config.isEnforcePresenceOfColumns());
        final Optional<String> combinedWarningMessages =
            H2OMojoPredictorUtils.combineFeatureColumnWarningMessages(warningMsgs);
        if (combinedWarningMessages.isPresent()) {
            setWarningMessage(combinedWarningMessages.get());
        }
    }

    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        //same for all models
        final DBDataPortObject inputData = (DBDataPortObject)inData[1];
        final DBSession session = inputData.getDBSession();

        final DataTableSpec outputSpec = getSpec(inputData.getDataTableSpec(), m_mojoSpec, m_config);
        final H2OMojoPortObject mojoPortObject = (H2OMojoPortObject)inData[0];

        final UDFObject udfObject = getUDFObject(inputData.getDataTableSpec(), outputSpec, mojoPortObject, m_config);

        exec.setMessage("Creating function");
        exec.checkCanceled();
        try (Connection connection = session.getConnectionProvider().getConnection(exec);
                Statement statement = connection.createStatement()) {
            exec.setMessage("Creating temporary stages");
            createTemporaryStageIfNotExists(exec.createSubProgress(0.1), statement, udfObject.getMojoStageName());
            createTemporaryStageIfNotExists(exec.createSubProgress(0.1), statement, UDFObject.FILES_STAGE_NAME);
            exec.checkCanceled();
            exec.setMessage("Uploading model files to Snowflake (this might take some time without progress changes)");
            uploadFilesIfNotExists(exec.createSubProgress(0.6), statement, udfObject.getStageToFiles());
            exec.setMessage("Register function");
            createUDF(exec.createSubProgress(0.1), statement, udfObject);
        }
        exec.setProgress(1, "Creating output query");
        final DBDataObject outputData = createOutputData(exec, inputData, udfObject, outputSpec);
        return new PortObject[]{new DBDataPortObject(inputData, outputData)};
    }

    private static void createTemporaryStageIfNotExists(final ExecutionMonitor exec, final Statement statement,
        final String stageName) throws SQLException {
        //when creating a space it needs to be in double quotes for special characters
        final String createTempStage = "CREATE TEMPORARY STAGE IF NOT EXISTS \"" + stageName + "\"";
        exec.setMessage("Stage name: " + stageName);
        statement.execute(createTempStage);
        exec.setProgress(1);
    }

    private static void uploadFilesIfNotExists(final ExecutionMonitor exec, final Statement statement,
        final Map<String, List<File>> stageToFiles)
        throws IOException, URISyntaxException, SQLException, CanceledExecutionException {

        try (FSConnection fsConnection = DefaultFSConnectionFactory.createLocalFSConnection();
                FSFileSystem<?> fs = fsConnection.getFileSystem()) {
            final URIExporter exporter =
                ((NoConfigURIExporterFactory)fsConnection.getURIExporterFactory(URIExporterIDs.KNIME_FILE))
                    .getExporter();
            final int fileCount = stageToFiles.entrySet().stream().mapToInt(e -> e.getValue().size()).sum();
            //upload model file
            int i = 0;
            for (Map.Entry<String, List<File>> entry : stageToFiles.entrySet()) {
                final String stage = entry.getKey();
                final List<File> files = entry.getValue();
                final Set<String> existingStageFileNames = retrieveStageAndFileNames(statement, stage);
                for (final File file : files) {
                    final String currentStageFileName = (stage + "/" + file.getName()).toLowerCase();
                    exec.setProgress(i / (double)fileCount, "Uploading file " + ++i + " of " + fileCount);
                    exec.checkCanceled();
                    if (!existingStageFileNames.contains(currentStageFileName)) {
                        final String fileURI = URIUtil.toUnencodedString(exporter.toUri(fs.getPath(file.toString())));
                        //see https://docs.snowflake.com/en/sql-reference/sql/put.html#required-parameters for path
                        //specification stage name must be in single quotes for special characters however Snowflake
                        //still converts the stage name to upper case!
                        final String putFileCommand =
                            "PUT '" + fileURI + "' '@" + stage + "' AUTO_COMPRESS=FALSE OVERWRITE = TRUE";
                        statement.execute(putFileCommand);
                    }
                }
            }
        }
        exec.setProgress(1);
    }

    private static Set<String> retrieveStageAndFileNames(final Statement statement, final String stage)
        throws SQLException {
        final Set<String> result = new HashSet<>();

        final String listFiles = "LS '@" + stage + "'";

        statement.execute(listFiles);
        try (ResultSet rs = statement.getResultSet()) {
            while (rs.next()) {
                final String stageAndFileName = rs.getString(COL_STAGED_FILE_NAME);
                if (StringUtils.isNotBlank(stageAndFileName)) {
                    result.add(stageAndFileName.toLowerCase());
                }
            }
        }
        return result;
    }

    private static void createUDF(final ExecutionMonitor exec, final Statement statement, final UDFObject udfObject)
        throws IOException, SQLException, InvalidSettingsException {

        //load the template from the companion jar and replace all placeholder
        final String udf = udfObject.buildUDFFromTemplate();

        exec.setMessage("Function name: " + udfObject.getFunctionName());
        statement.execute(udf);
        exec.setProgress(1);
    }

    private static DBDataObject createOutputData(final ExecutionContext exec, final DBDataPortObject inputData,
        final UDFObject udfObject, final DataTableSpec outputSpec)
        throws CanceledExecutionException, InvalidSettingsException, SQLException {

        final DBSession session = inputData.getDBSession();

        final String resultSQL = udfObject.isTabularUDF() ? createTUDFSQL(inputData, udfObject, outputSpec)
            : createUDFSQL(inputData, udfObject, outputSpec);

        final DBTypeMappingService<?, ?> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType());

        return session.getAgent(DBMetadataReader.class).getDBDataObject(exec, new SQLQuery(resultSQL), inputData
            .getExternalToKnimeTypeMapping().resolve(mappingService, DataTypeMappingDirection.EXTERNAL_TO_KNIME));
    }

    private static String createUDFSQL(final DBDataPortObject inputData, final UDFObject udfObject,
        final DataTableSpec outputSpec) {

        final DBSQLDialect dialect = inputData.getDBSession().getDialect();
        final String tempTableName = dialect.getTempTableName();
        final String predictColumnName = dialect.delimit(outputSpec.getColumnNames()[0]);
        final String inUdfColumns = delimitTableWithColumns(tempTableName, udfObject.getInUDFColumns(), dialect);

        return "SELECT *, " + udfObject.getFunctionName() + "(" + inUdfColumns + ") AS " + predictColumnName
            + "\nFROM (" + dialect.asTable(inputData.getData().getQuery() + ")", tempTableName);
    }

    private static String createTUDFSQL(final DBDataPortObject inputData, final UDFObject udfObject,
        final DataTableSpec outputSpec) {

        final DBSQLDialect dialect = inputData.getDBSession().getDialect();
        final String udfTableName = dialect.getTempTableName();
        final StringJoiner resultTableColumns = new StringJoiner(",");

        for (int i = 0; i < outputSpec.getNumColumns(); i++) {
            resultTableColumns
                .add(dialect.asColumn(dialect.delimit(udfTableName) + ".col" + i, outputSpec.getColumnNames()[i]));
        }

        final String tempTableName = dialect.getTempTableName();
        final String inTableColumns =
            delimitTableWithColumns(tempTableName, inputData.getDataTableSpec().getColumnNames(), dialect);
        final String inUdfColumns = delimitTableWithColumns(tempTableName, udfObject.getInUDFColumns(), dialect);

        return "SELECT " + inTableColumns + ", " + resultTableColumns + "\nFROM ("
            + dialect.asTable(inputData.getData().getQuery() + ")", tempTableName) + ",\n"
            + dialect.asTable("TABLE(" + udfObject.getFunctionName() + "(" + inUdfColumns + "))", udfTableName);
    }

    private static String delimitTableWithColumns(final String tableName, final String[] columnNames,
        final DBSQLDialect dialect) {

        final StringJoiner result = new StringJoiner(",");

        for (String columnName : columnNames) {
            result.add(dialect.delimit(tableName) + "." + dialect.delimit(columnName));
        }
        return result.toString();
    }

    /**
     * Constructs UDF object.
     *
     * @param inputTableSpec input table specification
     * @param resultTableSpec result table specification
     * @param mojoPortObject MOJO port object
     * @param config MOJO configuration
     * @return constructed UDF object
     * @throws InvalidSettingsException if input table doesn't contain MOJO column
     * @throws IOException if jar dependencies not found
     * @throws URISyntaxException if companion jar not found
     */
    protected UDFObject getUDFObject(final DataTableSpec inputTableSpec, final DataTableSpec resultTableSpec,
        final H2OMojoPortObject mojoPortObject, final H2OGeneralMojoPredictorConfig config)
        throws InvalidSettingsException, IOException, URISyntaxException {

        return new UDFObject(inputTableSpec, resultTableSpec, mojoPortObject, getPredictorClass(),
            m_config.isConvertUnknownCategoricalLevelsToNa(), m_config.isFailOnPredictException());
    }

    /**
     * Returns the {@link MojoPredictor} instance that should be used for prediction in the UDF.
     *
     * @return the {@link MojoPredictor} implementation to use in the UDF.
     */
    protected abstract Class<? extends MojoPredictor<?>> getPredictorClass();

    /**
     * Validates the specific configuration.
     *
     * @param tableSpec input database table specification
     * @param config MOJO configuration
     * @throws InvalidSettingsException if configuration is invalid
     */
    protected abstract void validateInternal(DataTableSpec tableSpec, H2OGeneralMojoPredictorConfig config)
        throws InvalidSettingsException;

    /**
     * Creates the specific configuration.
     *
     * @return configuration
     */
    protected abstract H2OGeneralMojoPredictorConfig createConfig();

    /**
     * Returns the specification of the output which will be created.
     *
     * @param spec input specification
     * @param mojoSpec MOJO specification
     * @param config configuration
     * @return output specification
     */
    protected abstract DataTableSpec getSpec(DataTableSpec spec, H2OMojoPortObjectSpec mojoSpec,
        H2OGeneralMojoPredictorConfig config);

    @Override
    protected void saveSettingsToInternal(final NodeSettingsWO settings) {
        m_config.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettingsInternal(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFromInternal(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config.loadValidatedSettingsFrom(settings);
    }
}
