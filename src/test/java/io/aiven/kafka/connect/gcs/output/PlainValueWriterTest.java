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

package io.aiven.kafka.connect.gcs.output;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class PlainValueWriterTest {


    // Test written by Diffblue Cover.
    @Test
    public void getOutputBytesInput0Output0() {

        // Arrange
        final PlainValueWriter plainValueWriter = new PlainValueWriter();
        final byte[] value = {};

        // Act
        final byte[] actual = plainValueWriter.getOutputBytes(value);

        // Assert result
        assertArrayEquals(new byte[] {}, actual);
    }

    // Test written by Diffblue Cover.
    @Test
    public void getOutputBytesInputNullOutputNull() {

        // Arrange
        final PlainValueWriter plainValueWriter = new PlainValueWriter();

        // Act and Assert result
        assertNull(plainValueWriter.getOutputBytes(null));
    }
}
