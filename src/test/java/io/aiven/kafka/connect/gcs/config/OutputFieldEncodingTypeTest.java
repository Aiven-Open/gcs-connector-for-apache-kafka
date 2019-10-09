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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OutputFieldEncodingTypeTest {


    // Test written by Diffblue Cover.
    @Test
    public void forNameInputNotNullOutputBase64() {

        // Act
        final OutputFieldEncodingType actual = OutputFieldEncodingType.forName("BAsE64");

        // Assert result
        assertEquals(OutputFieldEncodingType.BASE64, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void forNameInputNotNullOutputNone() {

        // Act
        final OutputFieldEncodingType actual = OutputFieldEncodingType.forName("nOnE");

        // Assert result
        assertEquals(OutputFieldEncodingType.NONE, actual);
    }
}
