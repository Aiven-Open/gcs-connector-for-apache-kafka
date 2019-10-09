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

public class OutputFieldTypeTest {

    // Test written by Diffblue Cover.
    @Test
    public void forNameInputNotNullOutputKey() {

        // Act
        final OutputFieldType actual = OutputFieldType.forName("KEY");

        // Assert result
        assertEquals(OutputFieldType.KEY, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void forNameInputNotNullOutputOffset() {

        // Act
        final OutputFieldType actual = OutputFieldType.forName("OFFSET");

        // Assert result
        assertEquals(OutputFieldType.OFFSET, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void forNameInputNotNullOutputTimestamp() {

        // Act
        final OutputFieldType actual = OutputFieldType.forName("TIMestamP");

        // Assert result
        assertEquals(OutputFieldType.TIMESTAMP, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void forNameInputNotNullOutputValue() {

        // Act
        final OutputFieldType actual = OutputFieldType.forName("VALue");

        // Assert result
        assertEquals(OutputFieldType.VALUE, actual);
    }
}
