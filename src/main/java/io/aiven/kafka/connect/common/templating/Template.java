/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.templating;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

/**
 * A simple templating engine that allows to bind variables to supplier functions.
 *
 * <p>Variable syntax: {@code {{ variable_name:parameter_name=parameter_value }}}.
 * Only alphanumeric characters and {@code _} are
 * allowed as a variable name.
 * Any number of spaces/tabs inside the braces is allowed.
 * Parameters for variable name are optional, same as for variable only alphanumeric characters
 * are allowed as a parameter name or a parameter value.
 *
 * <p>Non-bound variables are left as is.
 */
public final class Template {

    private static final Pattern VARIABLE_PATTERN =
        Pattern.compile("\\{\\{\\s*(([\\w_]+)(:([\\w]+)=([\\w]+))?)\\s*}}");

    private final List<Pair<String, Parameter>> variablesAndParameters;

    private final List<TemplatePart> templateParts;

    private final String originalTemplateString;

    private Template(final String template,
                     final List<Pair<String, Parameter>> variablesAndParameters,
                     final List<TemplatePart> templateParts) {
        this.originalTemplateString = template;
        this.variablesAndParameters = variablesAndParameters;
        this.templateParts = templateParts;
    }

    public String originalTemplate() {
        return originalTemplateString;
    }

    public final List<String> variables() {
        return variablesAndParameters.stream()
            .map(Pair::left)
            .collect(Collectors.toList());
    }

    public final Set<String> variablesSet() {
        return variablesAndParameters.stream().map(Pair::left).collect(Collectors.toSet());
    }

    public final List<Pair<String, Parameter>> variablesWithParameters() {
        return variablesAndParameters;
    }

    public final List<Pair<String, Parameter>>  variablesWithNonEmptyParameters() {
        return variablesAndParameters.stream()
            .filter(e -> !e.right().isEmpty())
            .collect(Collectors.toList());
    }

    public final Instance instance() {
        return new Instance();
    }

    public static Template of(final String template) {

        final Pair<List<Pair<String, Parameter>>, List<TemplatePart>>
            parsingResult = TemplateParser.parse(template);

        return new Template(template, parsingResult.left(), parsingResult.right());
    }

    @Override
    public String toString() {
        return originalTemplateString;
    }

    public final class Instance {
        private final Map<String, Function<Parameter, String>> bindings =
            new HashMap<>();

        private Instance() {
        }

        public final Instance bindVariable(final String name,
                                           final Supplier<String> binding) {
            return bindVariable(name, x -> binding.get());
        }

        public final Instance bindVariable(final String name,
                                           final Function<Parameter, String> binding) {
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(binding, "binding cannot be null");
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
                    final Function<Parameter, String> binding =
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
