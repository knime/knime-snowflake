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

import static java.util.Collections.unmodifiableMap;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.io.IOException;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.knime.bigdata.fileformats.parquet.ParquetFileFormatWriter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetTypeMappingService;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.database.DBTableSpec;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderFileFormat;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderSettings;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderStageType;
import org.knime.database.model.DBColumn;
import org.knime.database.model.DBTable;
import org.knime.database.node.io.load.ExecutionParameters;
import org.knime.database.node.io.load.impl.fs.DBFileLoadUtil.DBFileLoader;
import org.knime.database.node.io.load.impl.fs.util.DBMultiFileWriter;
import org.knime.database.node.io.load.impl.fs.util.DBRowWriter;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.filehandling.core.connections.FSPath;

/**
 * {@link DBFileLoader} implementation that writes out a Parquet file.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("deprecation")
public class SnowflakeParquetWriter
    extends DBMultiFileWriter<ConnectedSnowflakeLoaderNodeSettings, SnowflakeLoaderSettings> {

    private static final Map<SQLType, ParquetType> SNOWFLAKE_TO_PARQUET_TYPE_MAPPING;

    private static final int MEGABYTE = 1024 * 1024;

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

    private DataTypeMappingConfiguration<ParquetType> m_typeMappingConfiguration;

    private DataTableSpec m_spec;

    private CompressionCodecName m_compression;

    private int m_chunkSize;

    private long m_fileSize;

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
                if (JDBCType.NUMERIC.equals(sqlType)) {
                    throw new SQLException("Parquet not supported as exchange format for Snowflake tables with "
                        + "numeric/decimal columns. Please use CSV instead.");
                }
                throw new SQLException("Parquet type could not be found for the SQL type: " + sqlType);
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
    public SnowflakeLoaderSettings getLoadParameter(final ConnectedSnowflakeLoaderNodeSettings cs) {
        final SnowflakeLoaderFileFormat fileFormat =
            SnowflakeLoaderFileFormat.optionalValueOf(cs.getFileFormatSelectionModel().getStringValue())
                .orElseThrow(() -> new IllegalStateException("No file format is selected."));
        final SnowflakeLoaderStageType stageType =
            SnowflakeLoaderStageType.optionalValueOf(cs.getStageTypeSelectionModel().getStringValue())
                .orElseThrow(() -> new IllegalStateException("No stage type is selected."));
        return new SnowflakeLoaderSettings(fileFormat, cs.getFileFormatModel().getFileWriterSettings(), stageType,
            cs.getStageNameModel().getStringValue(), cs.getCompressionModel().getStringValue(),
            cs.getChunkSizeModel().getIntValue(), cs.getFileSizeModel().getLongValue());
    }

    @Override
    protected String getFilenameSuffix() {
        return ".parquet" + m_compression.getExtension();
    }

    @Override
    protected void setup(final ExecutionMonitor exec,
        final ExecutionParameters<ConnectedSnowflakeLoaderNodeSettings> parameters) throws Exception {
        final ConnectedSnowflakeLoaderNodeSettings cs = parameters.getCustomSettings();
        m_spec = parameters.getRowInput().getDataTableSpec();
        final DBSession session = parameters.getDBPortObject().getDBSession();
        final DBTable table = cs.getTableNameModel().toDBTable();
        final String compressionName = cs.getCompressionModel().getStringValue();
        if (SnowflakeLoaderFileFormat.NONE_COMPRESSION.equals(compressionName)) {
            m_compression = CompressionCodecName.UNCOMPRESSED;
        } else {
            m_compression = CompressionCodecName.valueOf(compressionName);
        }
        //user enters MB and Parquet expects bytes
        m_chunkSize = cs.getChunkSizeModel().getIntValue() * MEGABYTE;
        //user enters MB and Parquet expects bytes
        m_fileSize = cs.getFileSizeModel().getLongValue() * MEGABYTE;
        m_typeMappingConfiguration = createParquetTypeMappingConfiguration(m_spec, table, session, exec);
    }

    @SuppressWarnings("resource")
    @Override
    protected DBRowWriter getRowWriter(final ExecutionMonitor exec, final FSPath tempFolder) throws Exception {
        final ParquetFileFormatWriter writer = new ParquetFileFormatWriter(tempFolder, ParquetFileWriter.Mode.OVERWRITE,
            m_spec, m_compression, m_fileSize, m_chunkSize, m_typeMappingConfiguration);
        return new DBRowWriter() {
            @Override
            public void close() throws Exception {
                writer.close();
            }

            @Override
            public boolean writeRow(final DataRow row) throws IOException {
                return writer.writeRow(row);
            }
        };
    }
}
