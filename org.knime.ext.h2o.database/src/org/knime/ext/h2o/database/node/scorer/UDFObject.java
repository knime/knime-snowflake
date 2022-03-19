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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.UUID;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.URIUtil;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.vector.doublevector.DoubleVectorCellFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.ext.h2o.mojo.H2OMojoPortObject;
import org.knime.ext.h2o.mojo.H2OMojoPortObjectSpec;
import org.knime.snowflake.h2o.companion.udf.MojoPredictor;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.Version;

/**
 * Wrapper object for UDF.
 *
 * @author Zkriya Rakhimberdiyev
 */
public class UDFObject {

    /**This needs to be updated whenever a new companion jar version should be used!!!*/
    private static final String COMPANION_JAR = "lib/org.knime.snowflake.h2o.companion-1.0.1.jar";

    /** Name of the stage for storing dependencies. */
    static final String FILES_STAGE_NAME = "KNIME_UDF_JARS";

    private static List<File> getLatestH2OModelJars() throws IOException {
        final List<File> result = new ArrayList<>();
        for (final String symbolicName : List.of("ai.h2o.genmodel", "ai.h2o.logger",
            "ai.h2o.tree-api", "com.google.gson")) {
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
            result.add(FileLocator.getBundleFile(maxBundle));
        }
        return result;
    }

    private final DataTableSpec m_inputTableSpec;
    private final DataTableSpec m_resultTableSpec;
    private final Class<? extends MojoPredictor<?>> m_predictor;
    private final boolean m_convertUnknownCategoricalLevelsToNa;
    private final boolean m_failOnPredictException;

    private final String[] m_inUDFColumns;

    private final String m_mojoFileName;
    private final String m_mojoStageName;
    private final String m_functionName;

    private final boolean m_tabularUDF;
    private final Map<String, List<File>> m_stageToFiles;

    /**
     * Constructor wrapper object.
     *
     * @param inputTableSpec input table specification
     * @param resultTableSpec result table specification
     * @param mojoPortObject MOJO port object
     * @param predictor specific class of predictor
     * @param convertUnknownCategoricalLevelsToNa configuration whether to convert unknown categorical levels
     * @param failOnPredictException configuration whether to fail on predict exception
     * @throws InvalidSettingsException if input table doesn't contain MOJO column
     * @throws IOException if jar dependencies not found
     * @throws URISyntaxException if companion jar not found
     */
    public UDFObject(final DataTableSpec inputTableSpec, final DataTableSpec resultTableSpec,
        final H2OMojoPortObject mojoPortObject, final Class<? extends MojoPredictor<?>> predictor,
        final boolean convertUnknownCategoricalLevelsToNa, final boolean failOnPredictException)
                throws InvalidSettingsException, IOException, URISyntaxException {
        this(inputTableSpec, resultTableSpec, mojoPortObject, predictor, convertUnknownCategoricalLevelsToNa,
            failOnPredictException, (String[])null);
    }

    /**
     * Constructor wrapper object.
     *
     * @param inputTableSpec input table specification
     * @param resultTableSpec result table specification
     * @param mojoPortObject MOJO port object
     * @param predictor specific class of predictor
     * @param convertUnknownCategoricalLevelsToNa configuration whether to convert unknown categorical levels
     * @param failOnPredictException configuration whether to fail on predict exception
     * @param inputCols the names of the input columns to use during prediction or <code>null</code> if the columns
     * from the MOJO model should be used instead
     * @throws InvalidSettingsException if input table doesn't contain MOJO column
     * @throws IOException if jar dependencies not found
     * @throws URISyntaxException if companion jar not found
     */
    public UDFObject(final DataTableSpec inputTableSpec, final DataTableSpec resultTableSpec,
        final H2OMojoPortObject mojoPortObject, final Class<? extends MojoPredictor<?>> predictor,
        final boolean convertUnknownCategoricalLevelsToNa, final boolean failOnPredictException,
        final String... inputCols) throws InvalidSettingsException, IOException, URISyntaxException {

        m_inputTableSpec = inputTableSpec;
        m_resultTableSpec = resultTableSpec;
        m_predictor = predictor;
        m_convertUnknownCategoricalLevelsToNa = convertUnknownCategoricalLevelsToNa;
        m_failOnPredictException = failOnPredictException;

        final H2OMojoPortObjectSpec mojoSpec = mojoPortObject.getSpec();
        final File mojoModelFile = mojoPortObject.getFile();

        final String responseName = mojoSpec.getResponseName();
        final List<String> inUDFColumns = new ArrayList<>();

        final String[] inputColNames = inputCols != null ? inputCols : mojoSpec.getNames();
        for (String mojoColumnName : inputColNames) {
            if (!mojoColumnName.equals(responseName) && inputTableSpec.containsName(mojoColumnName)) {
                inUDFColumns.add(mojoColumnName);
            }
        }
        m_inUDFColumns = inUDFColumns.toArray(String[]::new);

        m_mojoFileName = mojoModelFile.getName();
        //Somehow the PUT command converts the stage name to upper case even though we enclose it into ''
        m_mojoStageName = ("KNIME_" + m_mojoFileName).toUpperCase();
        m_functionName = createUniqueFunctionName();

        m_tabularUDF = resultTableSpec.getNumColumns() > 1;

        final Map<String, List<File>> stageToFiles = new HashMap<>();
        stageToFiles.put(m_mojoStageName, List.of(mojoModelFile));

        final List<File> files2include = new ArrayList<>();
        files2include.addAll(getLatestH2OModelJars());
        files2include.add(getCompanionJar());
        stageToFiles.put(FILES_STAGE_NAME, files2include);

        m_stageToFiles = Collections.unmodifiableMap(stageToFiles);
    }

    private static String createUniqueFunctionName() {
        final UUID uuid = UUID.randomUUID();
        return ("f" + uuid.toString()).replace('-', '_');
    }

    private static String getSQLType(final DataColumnSpec spec) throws InvalidSettingsException {
        final DataType colType = spec.getType();
        if (colType == DoubleVectorCellFactory.TYPE) {
            return "ARRAY";
        } else if (colType.isCompatible(IntValue.class)) {
            return "INTEGER";
        } else if (colType.isCompatible(LongValue.class)) {
            return "BIGINT";
        } else if (colType.isCompatible(DoubleValue.class)) {
            return "DOUBLE";
        } else if (colType.isCompatible(StringValue.class)) {
            return "VARCHAR";
        } else {
            throw new InvalidSettingsException(
                "Column: " + spec.getName() + " has incompatibe SQL type: " + colType);
        }
    }

    private static String getJavaType(final DataColumnSpec spec) throws InvalidSettingsException {
        final DataType colType = spec.getType();

        if (colType == DoubleVectorCellFactory.TYPE) {
            return "double[]";
        } else if (colType.isCompatible(IntValue.class)) {
            return "Integer";
        } else if (colType.isCompatible(LongValue.class)) {
            return "Long";
        } else if (colType.isCompatible(DoubleValue.class)) {
            return "Double";
        } else if (colType.isCompatible(StringValue.class)) {
            return "String";
        } else {
            throw new InvalidSettingsException(
                "Column: " + spec.getName() + " has incompatibe Java type: " + colType);
        }
    }

    private File getCompanionJar() throws IOException, URISyntaxException {
        final Bundle currentBundle = FrameworkUtil.getBundle(getClass());
        URL url = FileLocator.find(currentBundle, new Path(COMPANION_JAR), null);

        //Lib files are packed in a jar. FileLocator.toFileURL will copy them to a temporary location
        //so that we can access them using File
        url = FileLocator.toFileURL(url);

        return URIUtil.toFile(URIUtil.toURI(url));
    }

    /**
     * Generates UDF from template.
     *
     * @return generated UDF
     * @throws IOException if template not found
     * @throws InvalidSettingsException if SQL or JAVA type is undefined
     */
    public String buildUDFFromTemplate() throws IOException, InvalidSettingsException {

        final String template = m_tabularUDF ? "tudfTemplate.sql" : "udfTemplate.sql";

        final StringJoiner inSQLArguments = new StringJoiner(",");
        final StringJoiner inParameters = new StringJoiner(",");
        final StringJoiner inVariables = new StringJoiner(",");

        for (int i = 0, length = m_inUDFColumns.length; i < length; i++) {
            final String colName = m_inUDFColumns[i];
            final DataColumnSpec spec = m_inputTableSpec.getColumnSpec(colName);

            inSQLArguments.add("v" + i + " " + getSQLType(spec));
            inParameters.add(getJavaType(spec) + " p" + i);
            inVariables.add("p" + i);
        }

        final StringJoiner imports = new StringJoiner(",");

        m_stageToFiles.forEach((stage, files) -> {
            for (final File file : files) {
                imports.add("'@" + stage + "/" + file.getName() + "'");
            }
        });

        final StringJoiner outSQLArguments = new StringJoiner(",");
        final StringJoiner fieldDeclarations = new StringJoiner(";");
        final StringJoiner fieldInitializations = new StringJoiner(";");

        String outSQLType = "";
        String javaResultType = "";

        for (int i = 0; i < m_resultTableSpec.getNumColumns(); i++) {
            final DataColumnSpec spec = m_resultTableSpec.getColumnSpec(i);
            outSQLArguments.add("col" + i + " " + getSQLType(spec));
            fieldDeclarations.add("public " + getJavaType(spec) + " col" + i);
            if (i == 0) {
                outSQLType = getSQLType(spec);
                javaResultType = getJavaType(spec);
                fieldInitializations.add("this.col" + i + " = prediction.getResult()");
            } else {
                fieldInitializations.add("this.col" + i + " = prediction.getClassProbabilities()[" + (i - 1) + "]");
            }
        }
        final StringJoiner columnNames = new StringJoiner(",");
        for (String columnName : m_inUDFColumns) {
            columnNames.add("\"" + columnName + "\"");
        }

        final Map<String, String> variables = new HashMap<>();

        variables.put("functionName", getFunctionName());
        variables.put("inSQLArguments", inSQLArguments.toString());

        variables.put("imports", imports.toString());
        variables.put("stageName", getMojoStageName());
        variables.put("fileName", m_mojoFileName);
        variables.put("mojoClass", m_predictor.getName());
        variables.put("columnNames", columnNames.toString());

        variables.put("outSQLType", outSQLType);
        variables.put("javaResultType", javaResultType);

        variables.put("fieldDeclarations", fieldDeclarations.toString() + ";");
        variables.put("fieldInitializations", fieldInitializations.toString() + ";");
        variables.put("outSQLArguments", outSQLArguments.toString());

        variables.put("inParameters", inParameters.toString());
        variables.put("inVariables", inVariables.toString());

        variables.put("convertUnknownCategoricalLevelsToNa",
            String.valueOf(m_convertUnknownCategoricalLevelsToNa));
        variables.put("failOnPredictException",
            String.valueOf(m_failOnPredictException));

        String udf;
        try (InputStream in = getClass().getResourceAsStream(template);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            final StringWriter stringWriter = new StringWriter();
            reader.transferTo(stringWriter);
            udf = stringWriter.toString();
            for (final Entry<String, String> entry : variables.entrySet()) {
                udf = udf.replace("<$" + entry.getKey() + "$>", entry.getValue());
            }
        }
        return udf;
    }

    /**
     * Gets the expected input column names for UDF.
     *
     * @return the columnNames for UDF
     */
    public String[] getInUDFColumns() {
        return m_inUDFColumns;
    }

    /**
     * Gets if UDF is tabular.
     *
     * @return true if UDF is tabular, otherwise false
     */
    public boolean isTabularUDF() {
        return m_tabularUDF;
    }

    /**
     * Gets stage name of MOJO.
     *
     * @return the mojoStageName
     */
    public String getMojoStageName() {
        return m_mojoStageName;
    }

    /**
     * Get map with stage to files.
     *
     * @return the stageToFiles map
     */
    public Map<String, List<File>> getStageToFiles() {
        return m_stageToFiles;
    }

    /**
     * Gets the name of the created UDF.
     *
     * @return the functionName
     */
    public String getFunctionName() {
        return m_functionName;
    }
}
