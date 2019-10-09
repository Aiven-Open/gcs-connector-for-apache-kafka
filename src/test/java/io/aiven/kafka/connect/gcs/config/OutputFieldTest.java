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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class OutputFieldTest {


    // Test written by Diffblue Cover.
    @Test
    public void equalsInputNullOutputFalse() {

        // Arrange
        final OutputField outputField =
            new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE);

        // Act and Assert result
        assertFalse(outputField.equals(null));
    }

    // Test written by Diffblue Cover.
    @Test
    public void getEncodingTypeOutputNone() {

        // Arrange
        final OutputField outputField =
            new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE);

        // Act
        final OutputFieldEncodingType actual = outputField.getEncodingType();

        // Assert result
        assertEquals(OutputFieldEncodingType.NONE, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getFieldTypeOutputKey() {

        // Arrange
        final OutputField outputField =
            new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE);

        // Act
        final OutputFieldType actual = outputField.getFieldType();

        // Assert result
        assertEquals(OutputFieldType.KEY, actual);
    }
}
