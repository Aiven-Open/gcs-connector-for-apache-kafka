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

package io.aiven.kafka.connect.gcs.output;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.Objects;

public final class ValueWriter implements OutputFieldWriter {
    /**
     * Takes the {@link SinkRecord}'s value as a byte array.
     *
     * <p>This assumes the value <b>is</b> a byte array.
     *
     * @param record the record to get the value from
     * @param outputStream the stream to write to
     * @throws DataException when the value is not actually a byte array
     */
    @Override
    public void write(final SinkRecord record,
                      final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record);
        Objects.requireNonNull(record.valueSchema());
        Objects.requireNonNull(record.value());
        Objects.requireNonNull(outputStream);

        if (record.valueSchema().type() != Schema.Type.BYTES) {
            final String msg = String.format("Record value schema type must be %s, %s given",
                    Schema.Type.BYTES, record.valueSchema().type());
            throw new DataException(msg);
        }

        if (!(record.value() instanceof byte[])) {
            throw new DataException("Value is not a byte array");
        }

        outputStream.write(Base64.getEncoder().encode((byte[]) record.value()));
    }
}
