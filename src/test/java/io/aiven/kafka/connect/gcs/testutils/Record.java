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

package io.aiven.kafka.connect.gcs.testutils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/*
Class that used for fixtures in tests.
Has converter to SinkRecord.
 */
public class Record {
    public String topic;
    public String key;
    public String value;
    public int partition;
    public int offset;
    public long timestamp;
    public Iterable<Header> headers;

    public Record(final String key,
                  final String value,
                  final Iterable<Header> headers) {
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    public Record(final String topic,
                  final String key,
                  final String value,
                  final int partition,
                  final int offset,
                  final long timestamp,
                  final Iterable<Header> headers) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    public static Record of(final String key,
                            final String value,
                            final Iterable<Header> headers) {
        return new Record(key, value, headers);
    }

    public static Record of(final String topic,
                            final String key,
                            final String value,
                            final int partition,
                            final int offset,
                            final int timestamp,
                            final Iterable<Header> headers) {
        return new Record(topic, key, value, partition, offset, timestamp, headers);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Record record = (Record) o;
        return Objects.equals(key, record.key)
                && Objects.equals(value, record.value)
                && headersEquals(headers, record.headers);
    }

    private boolean headersEquals(final Iterable<Header> h1, final Iterable<Header> h2) {
        final Iterator<Header> h1Iterator = h1.iterator();
        final Iterator<Header> h2Iterator = h2.iterator();
        while (h1Iterator.hasNext() && h2Iterator.hasNext()) {
            final Header header1 = h1Iterator.next();
            final Header header2 = h2Iterator.next();
            if (!Objects.equals(header1.key(), header2.key())) {
                return false;
            }
            if (!Objects.equals(header1.schema().type(), header2.schema().type())) {
                return false;
            }
            if (header1.schema().type() != Schema.Type.BYTES) {
                return false;
            }
            if (header2.schema().type() != Schema.Type.BYTES) {
                return false;
            }
            if (!Arrays.equals((byte[]) header1.value(), (byte[]) header2.value())) {
                return false;
            }
        }
        return !h1Iterator.hasNext() && !h2Iterator.hasNext();
    }

    public SinkRecord toSinkRecord() {
        return new SinkRecord(
                topic,
                partition,
                Schema.BYTES_SCHEMA,
                key.getBytes(StandardCharsets.UTF_8),
                Schema.BYTES_SCHEMA,
                value.getBytes(StandardCharsets.UTF_8),
                offset,
                timestamp,
                TimestampType.CREATE_TIME,
                headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, headers);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("key=").append(key).append(" ").append("value=").append(value).append("\n");
        for (final Header header : headers) {
            sb.append(header.key()).append(" ").append(Utils.bytesToHex((byte[]) header.value())).append("\n");
        }
        return sb.toString();
    }
}
