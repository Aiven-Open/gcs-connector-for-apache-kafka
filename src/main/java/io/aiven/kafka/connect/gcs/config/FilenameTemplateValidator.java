/*
 * Aiven Kafka GCS Connector
 * Copyright (c) 2019 Aiven Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.aiven.kafka.connect.gcs.config;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.gcs.templating.Template;
import io.aiven.kafka.connect.gcs.templating.VariableTemplatePart;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

final class FilenameTemplateValidator implements ConfigDef.Validator {

    private final String configName;

    private static final List<Set<String>> SUPPORTED_VARIABLES_SETS =
        ImmutableList.of(
            ImmutableSet.of(FilenameTemplateVariable.TOPIC.name,
                FilenameTemplateVariable.PARTITION.name,
                FilenameTemplateVariable.START_OFFSET.name),
            ImmutableSet.of(FilenameTemplateVariable.KEY.name)
        );

    private static final Map<String, Map<String, Set<String>>> SUPPORTED_VARIABLE_PARAMETERS_SET =
        ImmutableMap.of(
            FilenameTemplateVariable.START_OFFSET.name,
            ImmutableMap.of(
                FilenameTemplateVariable.START_OFFSET.parameterName,
                ImmutableSet.copyOf(FilenameTemplateVariable.START_OFFSET.parameterValues)
            )
        );

    FilenameTemplateValidator(final String configName) {
        this.configName = configName;
    }

    @Override
    public final void ensureValid(final String name, final Object value) {
        if (value == null) {
            return;
        }

        assert value instanceof String;

        // See https://cloud.google.com/storage/docs/naming
        final String valueStr = (String) value;
        if (valueStr.startsWith(".well-known/acme-challenge")) {
            throw new ConfigException(configName, value,
                "cannot start with '.well-known/acme-challenge'");
        }

        final Template template = Template.of((String) value);

        boolean isVariableSetSupported = false;
        for (final Set<String> supportedVariablesSet : SUPPORTED_VARIABLES_SETS) {
            if (supportedVariablesSet.equals(template.variablesSet())) {
                isVariableSetSupported = true;
                break;
            }
        }
        if (!isVariableSetSupported) {
            final String supportedSetsStr = SUPPORTED_VARIABLES_SETS.stream()
                .map(set -> String.join(",", set))
                .collect(Collectors.joining("; "));

            throw new ConfigException(configName, value,
                "unsupported set of template variables, supported sets are: " + supportedSetsStr);
        }

        boolean isVariableParametersSupported = true;
        for (final Map.Entry<String, Map<String, Set<String>>> e
            : SUPPORTED_VARIABLE_PARAMETERS_SET.entrySet()) {
            if (template.variableParameters().containsKey(e.getKey())) {
                final VariableTemplatePart.Parameter p =
                    template.variableParameters().get(e.getKey());
                if (e.getValue().containsKey(p.name())) {
                    isVariableParametersSupported =
                        e.getValue().get(p.name()).contains(p.value());
                }
            }
        }
        if (!isVariableParametersSupported) {
            final String supportedParametersSet = SUPPORTED_VARIABLE_PARAMETERS_SET.keySet().stream()
                .map(v -> FilenameTemplateVariable.of(v).parameterDescription())
                .collect(Collectors.joining(","));
            throw new ConfigException(configName, value,
                String.format(
                    "unsupported set of template variables parameters, supported sets are: %s",
                    supportedParametersSet)
            );
        }
    }
}
