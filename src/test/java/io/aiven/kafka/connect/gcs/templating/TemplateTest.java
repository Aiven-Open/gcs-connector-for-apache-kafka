/*
 * Aiven Kafka GCS Connector
 * Copyright (c) 2019 Aiven Ltd
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

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

final class TemplateTest {
    @Test
    void emptyString() {
        final Template te = new Template("");
        assertEquals("", te.instance().render());
    }

    @Test
    void noVariables() {
        final Template te = new Template("somestring");
        assertEquals("somestring", te.instance().render());
    }

    @Test
    void newLine() {
        final Template te = new Template("some\nstring");
        assertEquals("some\nstring", te.instance().render());
    }

    @Test
    void emptyVariableName() {
        final String templateStr = "foo{{ }}bar";
        final Template te = new Template(templateStr);
        final Template.Instance instance = te.instance();
        assertThrows(IllegalArgumentException.class, () -> instance.bindVariable("", () -> "foo"));
    }

    @Test
    void variableFormatNoSpaces() {
        final Template te = new Template("{{foo}}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("foo", instance.render());
    }

    @Test
    void variableFormatLeftSpace() {
        final Template te = new Template("{{ foo}}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("foo", instance.render());
    }

    @Test
    void variableFormatRightSpace() {
        final Template te = new Template("{{foo }}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("foo", instance.render());
    }

    @Test
    void variableFormatBothSpaces() {
        final Template te = new Template("{{ foo }}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("foo", instance.render());
    }

    @Test
    void variableFormatMultipleSpaces() {
        final Template te = new Template("{{   foo  }}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("foo", instance.render());
    }

    @Test
    void variableFormatTabs() {
        final Template te = new Template("{{\tfoo\t}}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("foo", instance.render());
    }

    @Test
    void variableUnderscoreAlone() {
        final Template te = new Template("{{ _ }}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("_", () -> "foo");
        assertEquals("foo", instance.render());
    }

    @Test
    void variableUnderscoreWithOtherSymbols() {
        final Template te = new Template("{{ foo_bar }}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo_bar", () -> "foo_bar");
        assertEquals("foo_bar", instance.render());
    }

    @Test
    void placeholderHasCurlyBracesInside() {
        final String templateStr = "{{ { }}";
        final Template te = new Template(templateStr);
        final Template.Instance instance = te.instance();
        instance.bindVariable("{", () -> "foo");
        assertEquals(templateStr, instance.render());
    }

    @Test
    void unclosedPlaceholder() {
        final String templateStr = "bb {{ aaa ";
        final Template te = new Template(templateStr);
        final Template.Instance instance = te.instance();
        instance.bindVariable("aaa", () -> "foo");
        assertEquals(templateStr, instance.render());
    }

    @Test
    void variableInBeginning() {
        final Template te = new Template("{{ foo }} END");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("foo END", instance.render());
    }

    @Test
    void variableInMiddle() {
        final Template te = new Template("BEGINNING {{ foo }} END");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("BEGINNING foo END", instance.render());
    }

    @Test
    void variableInEnd() {
        final Template te = new Template("BEGINNING {{ foo }}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("BEGINNING foo", instance.render());
    }

    @Test
    void nonBoundVariable() {
        final Template te = new Template("BEGINNING {{ foo }}");
        assertEquals("BEGINNING {{ foo }}", te.instance().render());
    }

    @Test
    void multipleVariables() {
        final Template te = new Template("1{{foo}}2{{bar}}3{{baz}}4");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        instance.bindVariable("bar", () -> "bar");
        instance.bindVariable("baz", () -> "baz");
        assertEquals("1foo2bar3baz4", instance.render());
    }

    @Test
    void sameVariableMultipleTimes() {
        final Template te = new Template("{{foo}}{{foo}}{{foo}}");
        final Template.Instance instance = te.instance();
        instance.bindVariable("foo", () -> "foo");
        assertEquals("foofoofoo", instance.render());
    }

    @Test
    void bigListOfNaughtyStringsJustString() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            final Template te = new Template(line);
            final Template.Instance instance = te.instance();
            assertEquals(line, instance.render());
        }
    }

    @Test
    void bigListOfNaughtyStringsWithVariableInBeginning() throws IOException, URISyntaxException {
        for (final String line : getBigListOfNaughtyStrings()) {
            final Template te = new Template("{{ foo }}" + line);
            final Template.Instance instance = te.instance();
            instance.bindVariable("foo", () -> "foo");
            assertEquals("foo" + line, instance.render());
        }
    }

    @Test
    void bigListOfNaughtyStringsWithVariableInEnd() throws IOException {
        for (final String line : getBigListOfNaughtyStrings()) {
            final Template te = new Template(line + "{{ foo }}");
            final Template.Instance instance = te.instance();
            instance.bindVariable("foo", () -> "foo");
            assertEquals(line + "foo", instance.render());
        }
    }

    private Collection<String> getBigListOfNaughtyStrings() throws IOException {
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("blns.txt");
             final InputStreamReader reader = new InputStreamReader(is);
             final BufferedReader bufferedReader = new BufferedReader(reader)) {

            return bufferedReader.lines().filter(s -> !s.isEmpty() && !s.startsWith("#"))
                    .collect(Collectors.toList());
        }
    }

    @Test
    void variables() {
        final Template te = new Template("1{{foo}}2{{bar}}3{{baz}}4");
        assertIterableEquals(Arrays.asList("foo", "bar", "baz"), te.variables());
    }
}
