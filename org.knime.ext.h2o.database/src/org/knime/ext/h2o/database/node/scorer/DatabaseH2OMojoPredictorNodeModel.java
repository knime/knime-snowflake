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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.URIUtil;
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
import org.knime.database.port.DBDataPortObject;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.ext.h2o.mojo.H2OMojoPortObject;
import org.knime.ext.h2o.mojo.H2OMojoPortObjectSpec;
import org.knime.filehandling.core.connections.DefaultFSConnectionFactory;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.noconfig.NoConfigURIExporterFactory;
import org.knime.snowflake.h2o.companion.udf.MojoPredictor;
import org.osgi.framework.Bundle;
import org.osgi.framework.Version;

/**
 * Basic node model for all Snowflake MOJO predictor nodes.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public abstract class DatabaseH2OMojoPredictorNodeModel extends DBNodeModel {

    /**
     * Constructor.
     */
    protected DatabaseH2OMojoPredictorNodeModel() {
        super(new PortType[]{H2OMojoPortObject.TYPE, DBDataPortObject.TYPE}, new PortType[]{DBDataPortObject.TYPE});
    }

    private static void addLatestH2OModelJars(final List<File> files) throws IOException {
        //TODO: Check if we really need the gson lib
        for (final String symbolicName : List.of("ai.h2o.genmodel", "ai.h2o.logger", "ai.h2o.tree-api",
            "com.google.gson")) {
            final Bundle[] bundles = Platform.getBundles(symbolicName, null);
            Version maxVersion = Version.emptyVersion;
            Bundle maxBundle = null;
            for (final Bundle bundle : bundles) {
                final Version version = bundle.getVersion();
                if (maxVersion.compareTo(version) < 0) {
                    maxVersion = version;
                    maxBundle = bundle;
                }
            }
            files.add(FileLocator.getBundleFile(maxBundle));
        }
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        //TODO: Check if MOJO model type is compatible and all model input columns are available
        //Have a look at H2OMojoPredictorUtils e.g. in AbstractSparkH2OMojoPredictorNodeModel
        return new PortObjectSpec[]{inSpecs[1]};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {

        //same for all models
        final DBDataPortObject inputData = (DBDataPortObject)inData[1];
        final DBSession session = inputData.getDBSession();

        final H2OMojoPortObject mojoPortObject = (H2OMojoPortObject)inData[0];
        final H2OMojoPortObjectSpec mojoSpec = mojoPortObject.getSpec();

        final File mojoModelFile = mojoPortObject.getFile();

        final List<File> files2include = new LinkedList<>();
        addLatestH2OModelJars(files2include);

        //TODO: Get the jar file from the lib folder via the class loader via load resources
        files2include.add(new File("C:\\DEV\\GIT\\master\\knime-snowflake\\org.knime.ext.h2o.database\\lib\\"
            + "org.knime.snowflake.h2o.companion-1.0.0.jar"));
        files2include.add(mojoModelFile);

        final String fileName = mojoModelFile.getName().toString();
        //Somehow the PUT command converts the stage name to upper case even though we enclose it into ''
        final String stageName = ("KNIME_" + fileName).toUpperCase();

        //TODO: Get entered name form dialog or use default
        final String predictionColName = "my prediction";

        final UDFArguments arguments = new UDFArguments(inputData, mojoSpec, stageName, fileName, getPredictorClass(),
            files2include, getJavaReturnType());

        exec.setMessage("Creating function");
        try (Connection connection = session.getConnectionProvider().getConnection(exec);
                Statement statement = connection.createStatement()) {

            createStage(exec.createSubProgress(0.2), statement, stageName);

            uploadFiles(exec.createSubProgress(0.4), statement, stageName, files2include);

            createUDF(exec.createSubProgress(0.4), statement, arguments);

        }
        final DBDataObject outputData = createOutputData(exec, inputData, arguments, predictionColName);
        return new PortObject[]{new DBDataPortObject(inputData, outputData)};
    }

    /**
     * Returns the Java class that is returned by the prediction method.
     *
     * @return the return type of the Java class
     */
    protected abstract Class<?> getJavaReturnType();

    /**
     * Returns the {@link MojoPredictor} instance that should be used for prediction in the UDF.
     *
     * @return the {@link MojoPredictor} implementation to use in the UDF.
     */
    protected abstract Class<? extends MojoPredictor> getPredictorClass();

    private static void createStage(final ExecutionMonitor exec, final Statement statement, final String stageName)
        throws SQLException {
        //when creating a space it needs to be in double quotes for special characters
        final String createTempStage = "CREATE OR REPLACE TEMPORARY STAGE \"" + stageName + "\"";
        exec.setMessage("Create temp stage: " + stageName);
        statement.execute(createTempStage);
        exec.setProgress(1);
    }

    private static void uploadFiles(final ExecutionMonitor exec, final Statement statement, final String stageName,
        final List<File> files) throws IOException, URISyntaxException, SQLException {
        try (FSConnection fsConnection = DefaultFSConnectionFactory.createLocalFSConnection();
                FSFileSystem<?> fs = fsConnection.getFileSystem();) {
            final URIExporter exporter =
                ((NoConfigURIExporterFactory)fsConnection.getURIExporterFactory(URIExporterIDs.KNIME_FILE))
                    .getExporter();
            //upload model file
            exec.setMessage("Uploading MOJO files to Snowflake (this might take some time without progress changes)");
            for (final File file : files) {
                final String fileURI = URIUtil.toUnencodedString(exporter.toUri(fs.getPath(file.toString())));
                //see https://docs.snowflake.com/en/sql-reference/sql/put.html#required-parameters for path
                //specification stage name must be in single quotes for special characters however Snowflake
                //still converts the stage name to upper case!
                final String putFileCommand =
                    "PUT '" + fileURI + "' '@" + stageName + "' AUTO_COMPRESS=FALSE OVERWRITE = TRUE";
                ////TODO: Use a different stage for the jar files and see if they already exists LS <STAGENAME>
                //prior overwriting them. But than we need to check if an existing jar is still up to date. To do so
                //we could use the MD5 check sum.
                statement.execute(putFileCommand);
            }
        }
        exec.setProgress(1);
    }

    private static void createUDF(final ExecutionMonitor exec, final Statement statement, final UDFArguments arguments)
        throws IOException, SQLException {

        ////TODO:Can we tie the UDFArguments class and the UDF template tighter together?
        final Map<String, String> variables = arguments.getVariables();


        //load the template from the companion jar and replace all placeholder
        //TODO: Maybe move the logic on how to create the udf from the template into the UDFArgumenets class and also
        //move the template here from the companion plugin
        String udf;
        final String packageName = MojoPredictor.class.getPackageName().replace('.', '/');
        try (InputStream in = MojoPredictor.class.getResourceAsStream("/" + packageName + "/udfTemplate.sql");
                BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            final StringWriter stringWriter = new StringWriter();
            reader.transferTo(stringWriter);
            udf = stringWriter.toString();
            for (final Entry<String, String> var : variables.entrySet()) {
                udf = udf.replace("<$" + var.getKey() + "$>", var.getValue());
            }
        }
        exec.setMessage("Create function: " + variables.get("functionName"));
        statement.execute(udf);
        exec.setProgress(1);
    }

    private static DBDataObject createOutputData(final ExecutionContext exec, final DBDataPortObject inputData,
        final UDFArguments arguments, final String predictionColName)
        throws CanceledExecutionException, InvalidSettingsException, SQLException {
        final DBSession session = inputData.getDBSession();
        final DBSQLDialect dialect = session.getDialect();
        final String predictionColumn = dialect.delimit(predictionColName);

        final String resultSQL =
            "SELECT *, " + arguments.getFunctionName() + "(" + arguments.getColumnNames() + ") AS " + predictionColumn
                + " FROM (" + dialect.asTable(inputData.getData().getQuery() + ")", dialect.getTempTableName());

        final DBTypeMappingService<?, ?> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType());
        final DBDataObject newData =
            session.getAgent(DBMetadataReader.class).getDBDataObject(exec, new SQLQuery(resultSQL), inputData
                .getExternalToKnimeTypeMapping().resolve(mappingService, DataTypeMappingDirection.EXTERNAL_TO_KNIME));
        return newData;
    }

    @Override
    protected void saveSettingsToInternal(final NodeSettingsWO settings) {

    }

    @Override
    protected void validateSettingsInternal(final NodeSettingsRO settings) throws InvalidSettingsException {

    }

    @Override
    protected void loadValidatedSettingsFromInternal(final NodeSettingsRO settings) throws InvalidSettingsException {

    }

}
