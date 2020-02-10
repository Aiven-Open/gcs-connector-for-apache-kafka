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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A simple templating engine that allows to bind variables to supplier functions.
 *
 * <p>Variable syntax: {@code {{ variable_name }}}. Only alphanumeric characters and {@code _} are
 * allowed as a variable name. Any number of spaces/tabs inside the braces is allowed.
 *
 * <p>Non-bound variables are left as is.
 */
public final class Template {
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{\\{\\s*([\\w_]+)\\s*}}"); // {{ var }}

    private final String originalTemplateString;

    private final List<String> variables = new ArrayList<>();
    private final List<TemplatePart> templateParts = new ArrayList<>();

    private final boolean usePaddingForKafkaOffset;

    public Template(final String template) {
        this(template, false);
    }

    public Template(final String template, final boolean usePaddingForKafkaOffset) {
        this.originalTemplateString = template;
        this.usePaddingForKafkaOffset = usePaddingForKafkaOffset;

        final Matcher m = VARIABLE_PATTERN.matcher(template);
        int position = 0;
        while (m.find()) {
            templateParts.add(new StaticTemplatePart(template.substring(position, m.start())));

            final String variableName = m.group(1);
            variables.add(variableName);
            templateParts.add(new DynamicTemplatePart(variableName, m.group()));
            position = m.end();
        }
        templateParts.add(new StaticTemplatePart(template.substring(position)));
    }

    public final boolean usePaddingForKafkaOffset() {
        return usePaddingForKafkaOffset;
    }

    public final List<String> variables() {
        return Collections.unmodifiableList(variables);
    }

    public final Set<String> variablesSet() {
        return Collections.unmodifiableSet(new HashSet<>(variables));
    }

    public final Instance instance() {
        return new Instance();
    }

    private abstract static class TemplatePart {
    }

    private static final class StaticTemplatePart extends TemplatePart {
        final String text;

        StaticTemplatePart(final String text) {
            this.text = text;
        }
    }

    private static final class DynamicTemplatePart extends TemplatePart {
        final String variableName;
        final String originalPlaceholder;

        DynamicTemplatePart(final String variableName, final String originalPlaceholder) {
            this.variableName = variableName;
            this.originalPlaceholder = originalPlaceholder;
        }
    }

    @Override
    public String toString() {
        return originalTemplateString;
    }

    public class Instance {
        private final Map<String, Supplier<String>> bindings = new HashMap<>();

        private Instance() {
        }

        public final Instance bindVariable(final String name, final Supplier<String> supplier) {
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(supplier, "supplier cannot be null");
            if (name.trim().isEmpty()) {
                throw new IllegalArgumentException("name must not be empty");
            }
            bindings.put(name, supplier);
            return this;
        }

        public final String render() {
            final StringBuilder sb = new StringBuilder();
            for (final TemplatePart templatePart : templateParts) {
                if (templatePart instanceof StaticTemplatePart) {
                    sb.append(((StaticTemplatePart) templatePart).text);
                } else if (templatePart instanceof DynamicTemplatePart) {
                    final DynamicTemplatePart dynamicTemplatePart = (DynamicTemplatePart) templatePart;
                    final Supplier<String> supplier = bindings.get(dynamicTemplatePart.variableName);
                    // Substitute for bound variables, pass the variable pattern as is for non-bound.
                    if (supplier != null) {
                        sb.append(supplier.get());
                    } else {
                        sb.append(dynamicTemplatePart.originalPlaceholder);
                    }
                }
            }
            return sb.toString();
        }
    }
}
