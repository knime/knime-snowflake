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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.commons.io.FileUtils.sizeOf;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.core.runtime.URIUtil;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.model.DBTable;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionReference;
import org.knime.filehandling.core.connections.DefaultFSConnectionFactory;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.noconfig.NoConfigURIExporterFactory;

/**
 * Snowflake data loader.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SnowflakeDBLoader implements DBLoader {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SnowflakeDBLoader.class);

    private final DBSessionReference m_sessionReference;

    /**
     * Constructs a {@link SnowflakeDBLoader} object.
     *
     * @param sessionReference the reference to the agent's session.
     */
    public SnowflakeDBLoader(final DBSessionReference sessionReference) {
        m_sessionReference = requireNonNull(sessionReference, "sessionReference");
    }

    @Override
    public void load(final ExecutionMonitor exec, final Object parameters) throws Exception {
        @SuppressWarnings("unchecked")
        final DBLoadTableFromFileParameters<SnowflakeLoaderSettings> loadParameters =
            (DBLoadTableFromFileParameters<SnowflakeLoaderSettings>)parameters;
        final String filePath = loadParameters.getFilePath();
        try (FSConnection fsConnection = DefaultFSConnectionFactory.createLocalFSConnection();
                FSFileSystem<?> fs = fsConnection.getFileSystem();) {
            final FSPath tempFile = fs.getPath(filePath);
            final List<FSPath> files;
            if (FSFiles.isDirectory(tempFile, LinkOption.NOFOLLOW_LINKS)) {
                files = FSFiles.getFilePathsFromFolder(tempFile);
            } else {
                files = List.of(tempFile);
            }
            copyAndLoadFile(exec, loadParameters, fsConnection, files);
        }
    }

    private void copyAndLoadFile(final ExecutionMonitor exec,
        final DBLoadTableFromFileParameters<SnowflakeLoaderSettings> loadParameters, final FSConnection fsConnection,
        final List<FSPath> tempFiles)
        throws URISyntaxException, CanceledExecutionException, SQLException, InvalidSettingsException {
        final DBTable table = loadParameters.getTable();
        final SnowflakeLoaderSettings additionalSettings = loadParameters.getAdditionalSettings()
            .orElseThrow(() -> new IllegalArgumentException("Missing additional settings."));
        final DBSession session = m_sessionReference.get();
        final DBSQLDialect dialect = session.getDialect();

        exec.setMessage("Loading data files into Snowflake...");
        final String stageName = getStageName(additionalSettings, table, dialect);
        final SnowflakeLoaderFileFormat fileFormat = additionalSettings.getFileFormat();
        //https://docs.snowflake.com/en/sql-reference/sql/put.html
        final String putParameter = fileFormat.getPutParameter(additionalSettings);
        //https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#optional-parameters
        final String copyParameter = fileFormat.getCopyParameter(additionalSettings);
        final List<String> stagedFileNames = new LinkedList<>();
        try (Connection connection = session.getConnectionProvider().getConnection(exec);
                Statement statement = connection.createStatement()) {
            final ExecutionMonitor subexec = exec.createSubProgress(0.4);
            int i = 1;
            final int fileCount = tempFiles.size();
            for (FSPath tempFile : tempFiles) {
                stagedFileNames.add(tempFile.getFileName().toString());
                final String fileURI = toLocalURI(fsConnection, tempFile);
                //https://docs.snowflake.com/en/sql-reference/sql/put.html#required-parameters for path specification
                final String putFileCommand = "PUT '" + fileURI + "' " + "'@" + stageName + "' " + putParameter;
                subexec.checkCanceled();
                subexec.setMessage(
                    format("Uploading file %d of %d of size %s (this might take some time without progress changes)", i,
                        fileCount, byteCountToDisplaySize(sizeOf(tempFile.toFile()))));
                statement.execute(putFileCommand);
                subexec.setProgress(i++ / (double)fileCount);
            }
            subexec.setProgress(1, "All data files successful loaded into Snowflake");
            //the purge command tells Snowflake to delete the file after successful loading so we don't need to do it
            //https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
            final String copyFileCommand = "COPY INTO " + dialect.createFullName(table) + " \nFROM '@" + stageName + "'"
                + createFilesList(stagedFileNames) + copyParameter + "\n PURGE=TRUE";
            exec.checkCanceled();
            exec.setMessage(
                "Loading staged data into Snowflake table (this might take some time without progress changes)");
            statement.execute(copyFileCommand);
            exec.setMessage("Data loaded successful into Snowflake table: " + table.toString());
            exec.setProgress(1);
        } catch (final Throwable throwable) {
            try (Connection connection = session.getConnectionProvider().getConnection(exec);
                    Statement statement = connection.createStatement()) {
                for (String stagedFileName : stagedFileNames) {
                    //try to remove the staged file only on exception since we use the purge option in the copy command
                    final String deleteFileCommand = "REMOVE " + "'@" + stageName + "/" + stagedFileName + "'";
                    exec.setMessage("Deleting staged file");
                    statement.execute(deleteFileCommand);
                }
            } catch (final Throwable t) {
                LOGGER.debug("Exception while removing staged file: " + t.getMessage());
            }
            throw new SQLException(throwable.getMessage(), throwable);
        }
    }

    private static String toLocalURI(final FSConnection fsConnection, final FSPath tempFile) throws Exception {
        final URIExporter exporter =
            ((NoConfigURIExporterFactory)fsConnection.getURIExporterFactory(URIExporterIDs.KNIME_FILE)).getExporter();
        final URI knimeUri = exporter.toUri(tempFile);
        final Path localPath = FileUtil.resolveToPath(knimeUri.toURL());
        return URIUtil.toUnencodedString(localPath.toUri());
    }

    private static String createFilesList(final List<String> stagedFileNames) {
        return " FILES=('" + String.join("','", stagedFileNames) + "') ";
    }

    private static String getStageName(final SnowflakeLoaderSettings additionalSettings, final DBTable table,
        final DBSQLDialect dialect) throws InvalidSettingsException {
        //stage name must be in single quotes for special characters see
        //https://docs.snowflake.com/en/sql-reference/sql/put.html#required-parameters
        final String userStageName = additionalSettings.getStageName();
        final String stageName;
        switch (additionalSettings.getStageType()) {
            case INTERNAL:
                stageName = dialect.createFullName(table.getCatalogName(), table.getSchemaName(), userStageName);
                break;
            case TABLE:
                //this is intentional since the % needs to be between the namespace and the identifier see
                //https://docs.snowflake.com/en/sql-reference/sql/put.html
                @SuppressWarnings("deprecation")
                final String nameSpace = dialect.createFullName(table.getCatalogName(), table.getSchemaName());
                stageName =
                    (StringUtils.isBlank(nameSpace) ? "" : nameSpace + ".") + "%" + dialect.delimit(table.getName());
                break;
            case USER:
                stageName = "~";
                break;
            default:
                throw new InvalidSettingsException("Unknown stage type: " + additionalSettings.getStageType());
        }
        return stageName;
    }

}
