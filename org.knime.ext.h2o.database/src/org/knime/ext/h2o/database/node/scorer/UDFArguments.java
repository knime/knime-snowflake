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
import java.util.List;
import java.util.Map;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.session.DBSession;
import org.knime.ext.h2o.mojo.H2OMojoPortObjectSpec;
import org.knime.snowflake.h2o.companion.udf.MojoPredictor;

/**
 * Class that collects all arguments e.g. column names, regression model, etc. to build the UDF.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class UDFArguments {

    private final String m_columnNames;

    private final String m_sqlVariables;

    private final String m_sqlReturnType;

    private final Class<?> m_javaReturnType;

    private final String m_javaParameter;

    private final String m_javaVariables;

    private final String m_stageName;

    private final String m_fileName;

    private final Class<? extends MojoPredictor> m_mojoClass;

    private final List<File> m_files2include;

    private final String m_functionName;

    /**
     * Constructor.
     *
     * @param dbDataObject {@link DBDataPortObject}
     * @param mojoSpec {@link H2OMojoPortObjectSpec}
     * @param stageName the Snowflake stage name
     * @param fileName the name of the MOJO model file
     * @param predictor the {@link MojoPredictor} implementation to use
     * @param files2include all the files to upload to the Snowflake stage
     * @param javaReturnType the return type of the Java predict method
     * @throws InvalidSettingsException if some settings are invalid
     */
    public UDFArguments(final DBDataPortObject dbDataObject, final H2OMojoPortObjectSpec mojoSpec,
        final String stageName, final String fileName, final Class<? extends MojoPredictor> predictor,
        final List<File> files2include, final Class<?> javaReturnType) throws InvalidSettingsException {
        m_stageName = stageName;
        m_fileName = fileName;
        m_mojoClass = predictor;
        m_files2include = files2include;
        m_functionName = ("f" + fileName).replace('-', '_');
        m_javaReturnType = javaReturnType;
        if (Double.class.equals(m_javaReturnType)) {
            m_sqlReturnType = "DOUBLE";
        } else if (String.class.equals(m_javaReturnType)) {
            m_sqlReturnType = "VARCHAR";
        } else {
            throw new InvalidSettingsException("Unsupported Java return type");
        }
        final DBSession session = dbDataObject.getDBSession();
        final DBSQLDialect dialect = session.getDialect();
        final DataTableSpec tableSpec = dbDataObject.getDataTableSpec();
        final String[] colNames = mojoSpec.getNames();
        final String responseName = mojoSpec.getResponseName();
        final StringBuilder colNameBuf = new StringBuilder();
        final StringBuilder sqlVarBuf = new StringBuilder();
        final StringBuilder javaParamBuf = new StringBuilder();
        final StringBuilder javaVariablesBuf = new StringBuilder();
        for (int i = 0, length = colNames.length; i < length; i++) {
            final String colName = colNames[i];
            if (!colName.equals(responseName)) {
                final DataColumnSpec spec = tableSpec.getColumnSpec(colName);
                if (spec == null) {
                    throw new InvalidSettingsException("Model column: " + colName + " not found in DB Data object");
                }
                if (i > 0) {
                    colNameBuf.append(", ");
                    sqlVarBuf.append(", ");
                    javaParamBuf.append(", ");
                    javaVariablesBuf.append(", ");
                }
                colNameBuf.append(dialect.delimit(colName));
                sqlVarBuf.append("v").append(i);
                final DataType colType = spec.getType();
                if (colType.isCompatible(DoubleValue.class)) {
                    sqlVarBuf.append(" DOUBLE");
                    javaParamBuf.append("Double ");
                } else if (spec.getType().isCompatible(StringValue.class)) {
                    sqlVarBuf.append(" VARCHAR");
                    javaParamBuf.append("String ");
                } else {
                    throw new InvalidSettingsException(
                        "Column: " + colName + " has incompatibe column type: " + colType);
                }
                javaParamBuf.append("p").append(i);
                javaVariablesBuf.append("p").append(i);
            }
        }
        m_columnNames = colNameBuf.toString();
        m_sqlVariables = sqlVarBuf.toString();
        m_javaParameter = javaParamBuf.toString();
        m_javaVariables = javaVariablesBuf.toString();
    }

    /**
     * Returns the variables map that is used to replace all variables in the UDF template.
     *
     * @return the variables map
     */
    public Map<String, String> getVariables() {
        final StringBuilder importsBuf = new StringBuilder();
        for (final File file : m_files2include) {
            if (importsBuf.length() > 0) {
                importsBuf.append(", ");
            }
            importsBuf.append("'@");
            importsBuf.append(m_stageName);
            importsBuf.append("/");
            importsBuf.append(file.getName());
            importsBuf.append("'");
        }

        //https://docs.snowflake.com/en/sql-reference/sql/put.html#required-parameters
        final Map<String, String> variables = Map.of("functionName", m_functionName, // enforce line break
            "arguments", m_sqlVariables, // enforce line break
            "sqlReturnType", m_sqlReturnType, // enforce line break
            "imports", importsBuf.toString(), // enforce line break
            "stageName", m_stageName, // enforce line break
            "fileName", m_fileName, // enforce line break
            "mojoClass", m_mojoClass.getName(), // enforce line break
            "javaReturnType", m_javaReturnType.getName(), // enforce line break
            "parameter", m_javaParameter, // enforce line break
            "variables", m_javaVariables);
        return variables;
    }

    /**
     * Returns the expected input column names.
     * @return the columnNames
     */
    public String getColumnNames() {
        return m_columnNames;
    }

    /**
     * Returns the name of the created UDF.
     *
     * @return the functionName
     */
    public String getFunctionName() {
        return m_functionName;
    }

}
