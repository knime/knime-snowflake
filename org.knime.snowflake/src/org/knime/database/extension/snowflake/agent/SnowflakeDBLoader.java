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

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.base.node.io.csvwriter.FileWriterSettings;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.model.DBTable;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionReference;

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
        final DBTable table = loadParameters.getTable();
        final SnowflakeLoaderSettings additionalSettings = loadParameters.getAdditionalSettings()
            .orElseThrow(() -> new IllegalArgumentException("Missing additional settings."));
        final DBSession session = m_sessionReference.get();
        final DBSQLDialect dialect = session.getDialect();

        final String userStageName = additionalSettings.getStageName();
        final String stageName;
        switch (additionalSettings.getStageType()) {
            case INTERNAL:
                stageName = dialect.createFullName(table.getCatalogName(), table.getSchemaName(), userStageName);
                break;
            case TABLE:
                //this is intentional since the % needs to be between the namespace and the identifier see
                //https://docs.snowflake.com/en/sql-reference/sql/put.html
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
        //https://docs.snowflake.com/en/sql-reference/sql/put.html
        final String putParameter;
        //https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#optional-parameters
        final String fileFormat;
        switch (additionalSettings.getFileFormat()) {
            case CSV:
                putParameter = " SOURCE_COMPRESSION=GZIP AUTO_COMPRESS=FALSE";

                final FileWriterSettings s = additionalSettings.getFileWriterSettings()
                    .orElseThrow(() -> new IllegalArgumentException("Missing file writer settings."));
                final String lineSeparator =
                    StringEscapeUtils.escapeJava(s.getLineEndingMode() == FileWriterSettings.LineEnding.SYST
                        ? System.getProperty("line.separator") : s.getLineEndingMode().getEndString());

                fileFormat = "\nFILE_FORMAT=(TYPE='CSV'" //enforced line break
                    + " COMPRESSION = GZIP" //enforced line break
                    + "\n RECORD_DELIMITER = '" + lineSeparator + "'" + "\n FIELD_DELIMITER = '" + s.getColSeparator()
                    + "'" //enforced line break
                    + "\n SKIP_HEADER = " + (s.writeColumnHeader() ? 1 : 0) //enforced line break
                    + "\n ESCAPE = "
                    + (StringUtils.isEmpty(s.getQuoteReplacement()) ? "NONE" : "'" + s.getQuoteReplacement() + "'")
                    + "\n FIELD_OPTIONALLY_ENCLOSED_BY  = '" + s.getQuoteBegin() + "'" //enforced line break
                    + "\n NULL_IF = '" + s.getMissValuePattern() + "'" //enforced line break
                    + "\n EMPTY_FIELD_AS_NULL  = FALSE" //enforced line break
                    + "\n ENCODING   = '" + s.getCharacterEncoding() + "'" //enforced line break
                    + "\n)";
                break;
            case PARQUET:
                putParameter = "";

                //https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
                fileFormat = "\nFILE_FORMAT=(TYPE='PARQUET') \nMATCH_BY_COLUMN_NAME=CASE_SENSITIVE";
                break;
            default:
                throw new IllegalArgumentException("Unsupported file format: " + additionalSettings.getFileFormat());
        }

        final File path = new File(filePath);
        final String fileName = path.getName();
        final String stagedFileName = "@" + stageName + "/" + fileName;
        final String fileURL = "file://" + filePath;

        final String putFileCommand = "PUT " + fileURL + " " + stagedFileName + putParameter;
        //the purge command tells Snowflake to delete the file after successful loading so we don't need to do it
        //https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
        final String copyFileCommand =
            "COPY INTO " + dialect.createFullName(table) + " \nFROM " + stagedFileName + fileFormat + "\n PURGE=TRUE";
        exec.checkCanceled();
        try (Connection connection = session.getConnectionProvider().getConnection(exec);
                Statement statement = connection.createStatement()) {
            exec.setMessage("Uploading data to Snowflake (this might take some time without progress changes)");
            statement.execute(putFileCommand);
            exec.setMessage("Loading staged data into table (this might take some time without progress changes)");
            statement.execute(copyFileCommand);
            exec.setProgress(1);
        } catch (final Throwable throwable) {
            //try to remove the staged file only on exception since we use the purge option in the copy command
            final String deleteFileCommand = "REMOVE " + stagedFileName;
            try (Connection connection = session.getConnectionProvider().getConnection(exec);
                    Statement statement = connection.createStatement()) {
                exec.setMessage("Deleting staged file");
                statement.execute(deleteFileCommand);
            } catch (final Throwable t) {
                LOGGER.debug("Exception while removing staged file: " + t.getMessage());
            }
            throw new SQLException(throwable.getMessage(), throwable);
        }
    }

}
