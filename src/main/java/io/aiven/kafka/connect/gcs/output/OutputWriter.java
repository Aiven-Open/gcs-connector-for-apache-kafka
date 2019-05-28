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

import io.aiven.kafka.connect.gcs.config.OutputField;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class OutputWriter {

    private static final byte[] FIELD_SEPARATOR = ",".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_SEPARATOR = "\n".getBytes(StandardCharsets.UTF_8);

    private final List<OutputFieldWriter> writers;

    private OutputWriter(final List<OutputFieldWriter> writers) {
        this.writers = writers;
    }

    public void writeRecord(final SinkRecord record,
                            final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record);
        Objects.requireNonNull(outputStream);
        writeFields(record, outputStream);
        outputStream.write(RECORD_SEPARATOR);
    }

    public void writeLastRecord(final SinkRecord record,
                                final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(record);
        Objects.requireNonNull(outputStream);
        writeFields(record, outputStream);
    }

    private void writeFields(final SinkRecord record,
                             final OutputStream outputStream) throws IOException {
        final Iterator<OutputFieldWriter> writerIter = writers.iterator();
        writerIter.next().write(record, outputStream);
        while (writerIter.hasNext()) {
            outputStream.write(FIELD_SEPARATOR);
            writerIter.next().write(record, outputStream);
        }
    }

    public static final class Builder {
        private final List<OutputFieldWriter> writers = new ArrayList<>();

        public final Builder addFields(final Collection<OutputField> fields) {
            Objects.requireNonNull(fields);

            for (final OutputField field : fields) {
                switch (field.getFieldType()) {
                    case KEY:
                        writers.add(new KeyWriter());
                        break;

                    case VALUE:
                        switch (field.getEncodingType()) {
                            case NONE:
                                writers.add(new PlainValueWriter());
                                break;

                            case BASE64:
                                writers.add(new Base64ValueWriter());
                                break;

                            default:
                                throw new ConnectException("Unknown output field encoding type " + field.getEncodingType());
                        }
                        break;

                    case OFFSET:
                        writers.add(new OffsetWriter());
                        break;

                    case TIMESTAMP:
                        writers.add(new TimestampWriter());
                        break;

                    default:
                        throw new ConnectException("Unknown output field type " + field);
                }
            }

            return this;
        }

        public final OutputWriter build() {
            return new OutputWriter(writers);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
