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

import java.util.List;
import java.util.function.Supplier;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Modification;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderFileFormat;
import org.knime.database.extension.snowflake.agent.SnowflakeLoaderStageType;
import org.knime.database.node.io.load.parameters.CSVFormatSettings;
import org.knime.database.node.io.load.parameters.CSVFormatSettings.CSVFormatSettingsModifier;
import org.knime.database.node.io.load.parameters.DBLoaderParametersBase;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;

/**
 * Web UI parameters for the Snowflake Loader node.
 *
 * @author Paul Baernreuther
 */
@SuppressWarnings("restriction")
@LoadDefaultsForAbsentFields
class SnowflakeLoaderNodeParameters extends DBLoaderParametersBase {

    static final class FileFormatRef implements ParameterReference<SnowflakeLoaderFileFormat> {
    }

    static final class FileFormatIsCSV implements EffectPredicateProvider {

        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(FileFormatRef.class).isOneOf(SnowflakeLoaderFileFormat.CSV);
        }
    }

    static final class FileFormatIsParquet implements EffectPredicateProvider {

        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(FileFormatRef.class).isOneOf(SnowflakeLoaderFileFormat.PARQUET);
        }
    }

    static final class RestrictCharacterEncodingAndExternalizeLayoutModification extends CSVFormatSettingsModifier {

        @Override
        protected Class<?> getExternalizedLayout() {
            return ExternalCSVFormatSection.class;
        }

        @Override
        protected Class<? extends StringChoicesProvider> getCharsetChoicesProvider() {
            return SnowflakeCharacterSetChoicesProvider.class;
        }

    }

    static final class SnowflakeCharacterSetChoicesProvider implements StringChoicesProvider {

        @Override
        public List<String> choices(final NodeParametersInput context) {
            return List.of("UTF-8", "ISO-8859-1");
        }

    }

    @Widget(title = "Stage Type", description = "The type of Snowflake stage to use for staging data files.")
    @Persist(configKey = "stageTypeSelection")
    @ValueReference(StageTypeRef.class)
    SnowflakeLoaderStageType m_stageType = SnowflakeLoaderStageType.USER;

    static final class StageTypeRef implements ParameterReference<SnowflakeLoaderStageType> {
    }

    static final class StageTypeIsInternal implements EffectPredicateProvider {

        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(StageTypeRef.class).isOneOf(SnowflakeLoaderStageType.INTERNAL);
        }
    }

    @Widget(title = "Internal stage name", description = "The name of the internal Snowflake stage.")
    @Persist(configKey = "stageName")
    @Effect(predicate = StageTypeIsInternal.class, type = EffectType.SHOW)
    String m_stageName = "";

    @Widget(title = "File Format", description = "The file format used to stage data before loading into Snowflake.")
    @ValueReference(FileFormatRef.class)
    SnowflakeLoaderFileFormat m_fileFormatSelection = SnowflakeLoaderFileFormat.CSV;

    public static final String GZIP_COMPRESSION = CompressionCodecName.GZIP.name();

    @Widget(title = "Compression", description = "The compression method to use for staging files.")
    @ChoicesProvider(CompressionChoicesProvider.class)
    @Persist(configKey = "fileCompression")
    String m_compression = GZIP_COMPRESSION;

    static final class CompressionChoicesProvider implements StringChoicesProvider {

        Supplier<SnowflakeLoaderFileFormat> m_fileFormat;

        @Override
        public void init(final StateProviderInitializer initializer) {
            StringChoicesProvider.super.init(initializer);
            m_fileFormat = initializer.computeFromValueSupplier(FileFormatRef.class);
        }

        @Override
        public List<String> choices(final NodeParametersInput context) {
            SnowflakeLoaderFileFormat format = m_fileFormat.get();
            return format.getCompressionFormats();
        }

    }

    abstract static class FileFormatToNumberProvider<T> implements StateProvider<T> {

        private Supplier<SnowflakeLoaderFileFormat> m_fileFormatSupplier;

        abstract T getForFileFormat(SnowflakeLoaderFileFormat format);

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_fileFormatSupplier = initializer.computeFromValueSupplier(FileFormatRef.class);
        }

        @Override
        public T computeState(final NodeParametersInput parametersInput) throws StateComputationFailureException {
            final var format = m_fileFormatSupplier.get();
            return getForFileFormat(format);
        }

    }

    static final class WithinFileChunkSizeProvider extends FileFormatToNumberProvider<Integer> {

        @Override
        Integer getForFileFormat(final SnowflakeLoaderFileFormat format) {
            return format.getDefaultChunkSize();
        }

    }

    static final class FileSizeProvider extends FileFormatToNumberProvider<Long> {

        @Override
        Long getForFileFormat(final SnowflakeLoaderFileFormat format) {
            return format.getDefaultFileSize();
        }

    }

    @ValueProvider(WithinFileChunkSizeProvider.class)
    @Widget(title = "Within file chunk size", description = "Within file chunk size (MB) for Parquet files.")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @Persist(configKey = "withinFileChunkSize")
    @Effect(predicate = FileFormatIsParquet.class, type = EffectType.SHOW)
    int m_chunkSize = SnowflakeLoaderFileFormat.PARQUET.getDefaultChunkSize();

    @ValueProvider(FileSizeProvider.class)
    @Widget(title = "File Size", description = "Split data into files of size (MB) for Parquet files.")
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @Persist(configKey = "fileSize")
    @Effect(predicate = FileFormatIsParquet.class, type = EffectType.SHOW)
    long m_fileSize = SnowflakeLoaderFileFormat.PARQUET.getDefaultFileSize();

    @Effect(predicate = FileFormatIsCSV.class, type = EffectType.SHOW)
    @Advanced
    @Section(title = "CSV Format Settings")
    interface ExternalCSVFormatSection {
    }

    @Modification(RestrictCharacterEncodingAndExternalizeLayoutModification.class)
    CSVFormatSettings m_fileFormat = new CSVFormatSettings();

}
