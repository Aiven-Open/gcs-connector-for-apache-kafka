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

package io.aiven.kafka.connect.gcs.templating;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * A simple templating engine that allows to bind variables to supplier functions.
 *
 * <p>Variable syntax: {@code {{ variable_name }}}. Only alphanumeric characters and {@code _} are
 * allowed as a variable name. Any number of spaces/tabs inside the braces is allowed.
 *
 * <p>Non-bound variables are left as is.
 */
public final class Template {

    private static final Pattern VARIABLE_PATTERN =
        Pattern.compile("\\{\\{\\s*(([\\w_]+)(:([\\w]+)=([\\w]+))?)\\s*}}");

    private final List<String> variables;

    private final List<TemplatePart> templateParts;

    private final String originalTemplateString;

    private Template(final String template,
                     final List<String> variables,
                     final List<TemplatePart> templateParts) {
        this.originalTemplateString = template;
        this.variables = variables;
        this.templateParts = templateParts;
    }

    public final List<String> variables() {
        return variables;
    }

    public final Set<String> variablesSet() {
        return ImmutableSet.copyOf(variables);
    }

    public final Map<String, VariableTemplatePart.Parameter> variableParameters() {
        return templateParts
            .stream()
            .filter(tp -> tp instanceof VariableTemplatePart)
            .map(tp -> (VariableTemplatePart) tp)
            .collect(Collectors.toMap(VariableTemplatePart::variableName, VariableTemplatePart::parameter));
    }

    public final Instance instance() {
        return new Instance();
    }

    public static Template of(final String template) {
        final ImmutableList.Builder<String> imVariableBuilder =
            ImmutableList.builder();
        final ImmutableList.Builder<TemplatePart> imTemplateParts =
            ImmutableList.builder();
        final Matcher m = VARIABLE_PATTERN.matcher(template);
        int position = 0;
        while (m.find()) {
            imTemplateParts.add(new TextTemplatePart(template.substring(position, m.start())));
            final String variableName = m.group(2);
            imVariableBuilder.add(variableName);
            final VariableTemplatePart.Parameter p =
                VariableTemplatePart.Parameter.of(m.group(4), m.group(5));
            imTemplateParts.add(new VariableTemplatePart(variableName, p, m.group()));
            position = m.end();
        }
        imTemplateParts.add(new TextTemplatePart(template.substring(position)));
        return new Template(
            template,
            imVariableBuilder.build(),
            imTemplateParts.build()
        );
    }

    @Override
    public String toString() {
        return originalTemplateString;
    }

    public final class Instance {
        private final Map<String, Function<VariableTemplatePart.Parameter, String>> bindings =
            new HashMap<>();

        private Instance() {
        }

        public final Instance bindVariable(final String name,
                                           final Supplier<String> binding) {
            return bindVariable(name, x -> binding.get());
        }

        public final Instance bindVariable(final String name,
                                           final Function<VariableTemplatePart.Parameter, String> binding) {
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(binding, "supplier cannot be null");
            if (name.trim().isEmpty()) {
                throw new IllegalArgumentException("name must not be empty");
            }
            bindings.put(name, binding);
            return this;
        }

        public final String render() {
            final StringBuilder sb = new StringBuilder();
            //FIXME we need better solution instead of instanceof
            for (final TemplatePart templatePart : templateParts) {
                if (templatePart instanceof TextTemplatePart) {
                    sb.append(((TextTemplatePart) templatePart).text());
                } else if (templatePart instanceof VariableTemplatePart) {
                    final VariableTemplatePart variableTemplatePart = (VariableTemplatePart) templatePart;
                    final Function<VariableTemplatePart.Parameter, String> binding =
                        bindings.get(variableTemplatePart.variableName());
                    // Substitute for bound variables, pass the variable pattern as is for non-bound.
                    if (Objects.nonNull(binding)) {
                        sb.append(binding.apply(variableTemplatePart.parameter()));
                    } else {
                        sb.append(variableTemplatePart.originalPlaceholder());
                    }
                }
            }
            return sb.toString();
        }
    }
}
