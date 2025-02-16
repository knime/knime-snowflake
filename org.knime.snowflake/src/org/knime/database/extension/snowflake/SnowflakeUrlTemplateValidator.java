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
package org.knime.database.extension.snowflake;

import static java.util.Collections.unmodifiableMap;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_DATABASE;
import static org.knime.database.driver.URLTemplates.VARIABLE_NAME_SCHEMA;
import static org.knime.database.driver.URLTemplates.validateDriverUrlTemplate;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ACCOUNT_DOMAIN;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ACCOUNT_NAME;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_ROLE;
import static org.knime.database.extension.snowflake.util.SnowflakeAbstractDriverLocator.VARIABLE_NAME_WAREHOUSE;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExecutableExtension;
import org.eclipse.core.runtime.IExecutableExtensionFactory;
import org.knime.core.node.NodeLogger;
import org.knime.database.UrlTemplateValidator;
import org.knime.database.node.connector.AbstractUrlTemplateValidator;
import org.knime.database.util.NestedTokenException;
import org.knime.database.util.NoSuchTokenException;
import org.knime.database.util.StringTokenException;

/**
 * {@link UrlTemplateValidator} implementation that validates server URL templates based on the common database server
 * connector inputs.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class SnowflakeUrlTemplateValidator extends AbstractUrlTemplateValidator implements IExecutableExtension {

    /**
     * Executable extension factory that prevents the creation of multiple {@link SnowflakeUrlTemplateValidator}
     * objects by the executable extension creation process. Although multiple factory objects are still created,
     * those instances can be discarded after the production of the singleton result.
     *
     * @author Noemi Balassa
     */
    public static class ExecutableExtensionFactory implements IExecutableExtensionFactory {
        @Override
        public Object create() throws CoreException {
            return INSTANCE;
        }
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SnowflakeUrlTemplateValidator.class);

    private static final Map<String, String> VARIABLES;
    static {
        final Map<String, String> variables = new LinkedHashMap<String, String>();
        variables.put(VARIABLE_NAME_ACCOUNT_NAME, "The Snowflake account name.");
        variables.put(VARIABLE_NAME_ACCOUNT_DOMAIN,
                "The domain part of your Snowflake account (e.g. snowflakecomputing.com).");
        variables.put(VARIABLE_NAME_WAREHOUSE, "The virtual warehouse to use once connected.");
        variables.put(VARIABLE_NAME_ROLE,
            "The optional default access control role to use in the Snowflake session initiated by the driver.");
        variables.put(VARIABLE_NAME_DATABASE, "The optional default database to use once connected.");
        variables.put(VARIABLE_NAME_SCHEMA,
            "The optional default schema to use for the specified database once connected.");
        VARIABLES = unmodifiableMap(variables);
    }

    private static final Set<String> VARIABLE_NAMES = VARIABLES.keySet();

    /**
     * The singleton {@link SnowflakeUrlTemplateValidator} instance.
     */
    static final SnowflakeUrlTemplateValidator INSTANCE = new SnowflakeUrlTemplateValidator();

    @Override
    public void setInitializationData(final IConfigurationElement config, final String propertyName, final Object data)
        throws CoreException {
        try {
            setExamplesFrom(config, false);
        } catch (final Throwable throwable) {
            LOGGER.error("The examples of an URL template validator could not be set in the extension: "
                + config.getDeclaringExtension().getNamespaceIdentifier(), throwable);
        }
    }

    @Override
    public Map<String, String> getConditionVariables() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getTokenVariables() {
        return VARIABLES;
    }

    @Override
    public Optional<String> validate(final CharSequence urlTemplate) {
        try {
            validateDriverUrlTemplate(urlTemplate, Collections.emptySet(), VARIABLE_NAMES);
        } catch (final NestedTokenException exception) {
            final String token = exception.getToken();
            return Optional
                .of("Illegally nested variable, or illegally nested token in the content of the condition: \""
                    + (token == null ? "" : token) + '"');
        } catch (final NoSuchTokenException exception) {
            final String token = exception.getToken();
            return Optional.of("Invalid variable: \"" + (token == null ? "" : token) + '"');
        } catch (final StringTokenException exception) {
            final String message = exception.getMessage();
            return Optional.of(message == null ? "Erroneous template." : message);
        } catch (final Throwable throwable) {
            LOGGER.error("An error occurred during URL template validation.", throwable);
        }
        return Optional.empty();
    }

}
