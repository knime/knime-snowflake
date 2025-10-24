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

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.base.node.io.csvwriter.FileWriterSettings;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.database.extension.snowflake.node.io.load.writer.ConnectedSnowflakeLoaderNodeSettings;
import org.knime.database.extension.snowflake.node.io.load.writer.SnowflakeCsvWriter;
import org.knime.database.extension.snowflake.node.io.load.writer.SnowflakeParquetWriter;
import org.knime.database.node.io.load.impl.fs.util.DBFileWriter;
import org.knime.node.parameters.widget.choices.Label;

/**
 * The intermediate file formats supported by the Snowflake data loader node.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
/**
 * The supported file formats.
 *
 * @author Tobias
 */
@SuppressWarnings("deprecation")
public enum SnowflakeLoaderFileFormat implements ButtonGroupEnumInterface {
        /**
         * CSV file format.
         */
        @Label(value = "CSV", description = "Comma-separated values")
        CSV("CSV", "Comma-separated values", ".csv"),
        /**
         * Apache Parquet file format.
         */
        @Label(value = "Parquet", description = "Apache Parquet")
        PARQUET("Parquet", "Apache Parquet", ".parquet");

    private static final String SNAPPY_COMPRESSION = CompressionCodecName.SNAPPY.name();

    /** GZIP compression flag which is available for all file formats. */
    public static final String GZIP_COMPRESSION = CompressionCodecName.GZIP.name();

    /** No compression flag which is available for all file formats. */
    public static final String NONE_COMPRESSION = "NONE";

    /**
     * Gets the {@link SnowflakeLoaderFileFormat} constant with the specified name.
     *
     * @param name the name of the constant.
     * @return {@linkplain Optional optionally} the {@link SnowflakeLoaderFileFormat} constant with the specified name
     *         or {@linkplain Optional#empty() empty}.
     */
    public static Optional<SnowflakeLoaderFileFormat> optionalValueOf(final String name) {
        if (name != null) {
            try {
                return Optional.of(valueOf(name));
            } catch (IllegalArgumentException exception) {
                // Ignored.
            }
        }
        return Optional.empty();
    }

    /**
     * Gets the default stage type.
     *
     * @return the default stage type
     */
    public static SnowflakeLoaderFileFormat getDefault() {
        for (SnowflakeLoaderFileFormat t : values()) {
            if (t.isDefault()) {
                return t;
            }
        }
        return CSV;
    }

    private final String m_fileExtension;

    private final String m_text;

    private final String m_toolTip;

    /**
     * Constructor.
     *
     * @param text text
     * @param toolTip tool tip
     * @param fileExtension file extension
     */
    SnowflakeLoaderFileFormat(final String text, final String toolTip, final String fileExtension) {
        m_text = text;
        m_toolTip = toolTip;
        m_fileExtension = fileExtension;
    }

    /**
     * Gets the file extension of the format.
     *
     * @return a file extension string, e.g. {@code ".txt"}.
     */
    public String getFileExtension() {
        return m_fileExtension;
    }

    /**
     * Returns the {@link DBFileWriter} to use.
     *
     * @return the {@link DBFileWriter} to use
     */
    public DBFileWriter<ConnectedSnowflakeLoaderNodeSettings, SnowflakeLoaderSettings> getWriter() {
        switch (this) {
            case CSV:
                return new SnowflakeCsvWriter();
            case PARQUET:
                return new SnowflakeParquetWriter();
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * Returns the list of supported compression formats.
     *
     * @return the list of supported compression formats
     */

    public List<String> getCompressionFormats() {
        final List<String> compressionFormats = new LinkedList<>();
        compressionFormats.add(NONE_COMPRESSION);
        switch (this) {
            case CSV:
                compressionFormats.add(GZIP_COMPRESSION);
                break;
            case PARQUET:
                compressionFormats.add(GZIP_COMPRESSION);
                compressionFormats.add(SNAPPY_COMPRESSION);
                break;
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
        return compressionFormats;

    }

    /**
     * Returns the chunk size tool tip.
     *
     * @return the chunk size tool tip per format
     */
    public String getChunkSizeToolTipText() {
        switch (this) {
            case CSV:
                return "Not supported";
            case PARQUET:
                return "Within file Row Group size (MB)";
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * Returns the file size per format.
     *
     * @return the file size tool tip per format
     */
    public String getFileSizeToolTipText() {
        switch (this) {
            case CSV:
                return "Not supported";
            case PARQUET:
                return "Split data into files of size (MB)";
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * Returns the default compression format.
     *
     * @return the default compression format
     */
    public String getDefaultCompressionFormat() {
        switch (this) {
            case CSV:
                return GZIP_COMPRESSION;
            case PARQUET:
                return SNAPPY_COMPRESSION;
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * Returns the default chunk size.
     *
     * @return the default chunk size per format
     */
    public int getDefaultChunkSize() {
        switch (this) {
            case CSV:
                return 1024;
            case PARQUET:
                return 128;
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    /**
     * Returns the default file size.
     *
     * @return the default file size per format
     */
    public long getDefaultFileSize() {
        switch (this) {
            case CSV:
                return 1024;
            case PARQUET:
                return 1024;
            default:
                throw new IllegalStateException("Unsupported file format: " + this.name());
        }
    }

    @Override
    public String getText() {
        return m_text;
    }

    @Override
    public String getActionCommand() {
        return name();
    }

    @Override
    public String getToolTip() {
        return m_toolTip;
    }

    @Override
    public boolean isDefault() {
        return this == CSV;
    }

    /**
     * Returns the put parameter part put command.
     *
     * @param settings {@link SnowflakeLoaderSettings} parameter
     * @return the put parameter
     */
    String getPutParameter(final SnowflakeLoaderSettings settings) {
        //https://docs.snowflake.com/en/sql-reference/sql/put.html
        if (GZIP_COMPRESSION.equals(settings.getCompression())) {
            return " SOURCE_COMPRESSION=GZIP AUTO_COMPRESS=FALSE";
        }
        return " AUTO_COMPRESS=FALSE";
    }

    /**
     * Returns the file format dependent part of the copy file command.
     *
     * @param settings the user settings
     * @return the file format part of the load command
     */
    String getCopyParameter(final SnowflakeLoaderSettings settings) {
        //https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#optional-parameters
        switch (this) {
            case CSV:

                final FileWriterSettings s = settings.getFileWriterSettings()
                    .orElseThrow(() -> new IllegalArgumentException("Missing file writer settings."));
                final String lineSeparator =
                    StringEscapeUtils.escapeJava(s.getLineEndingMode() == FileWriterSettings.LineEnding.SYST
                        ? System.getProperty("line.separator") : s.getLineEndingMode().getEndString());
                final String compression;
                if (GZIP_COMPRESSION.equals(settings.getCompression())) {
                    compression = " COMPRESSION = GZIP";
                } else {
                    compression = "";
                }
                return "\nFILE_FORMAT=(TYPE='CSV'" //enforced line break
                    + compression //enforced line break
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
            case PARQUET:

                //https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions
                return "\nFILE_FORMAT=(TYPE='PARQUET') \nMATCH_BY_COLUMN_NAME=CASE_SENSITIVE";
            default:
                throw new IllegalArgumentException("Unsupported file format: " + this);
        }
    }

}
