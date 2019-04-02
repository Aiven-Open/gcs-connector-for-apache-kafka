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

package io.aiven.kafka.connect.gcs;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.aiven.kafka.connect.gcs.config.CompressionType;
import io.aiven.kafka.connect.gcs.config.GcsSinkConfig;
import io.aiven.kafka.connect.gcs.output.OutputWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.GZIPOutputStream;

public final class GcsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(GcsSinkConnector.class);

    private final Map<TopicPartition, List<SinkRecord>> buffers = new HashMap<>();

    private OutputWriter outputWriter;

    private GcsSinkConfig config;
    private Storage storage;

    // required by Connect
    public GcsSinkTask() { }

    // for testing
    GcsSinkTask(final Map<String, String> props,
                final Storage storage) {
        Objects.requireNonNull(props);
        Objects.requireNonNull(storage);

        this.config = new GcsSinkConfig(props);
        this.storage = storage;
        this.outputWriter = OutputWriter.builder().addFields(config.getOutputFields()).build();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props);

        this.config = new GcsSinkConfig(props);
        this.storage = StorageOptions.getDefaultInstance().getService();
        outputWriter = OutputWriter.builder().addFields(config.getOutputFields()).build();
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records);

        log.info("Processing {} records", records.size());
        for (final SinkRecord record : records) {
            final TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            if (!buffers.containsKey(tp)) {
                buffers.put(tp, new ArrayList<>());
            }
            buffers.get(tp).add(record);
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        for (final Map.Entry<TopicPartition, List<SinkRecord>> entry : buffers.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final List<SinkRecord> buffer = entry.getValue();

            if (buffer.isEmpty()) {
                continue;
            }

            final SinkRecord firstRecord = buffer.get(0);
            final String filename = createFilename(tp, firstRecord);
            final BlobInfo blob = BlobInfo
                    .newBuilder(config.getBucketName(), filename)
                    .build();

            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                // Don't group these two tris,
                // because the internal one must be closed before writing to GCS.
                try (final OutputStream compressedStream = getCompressedStream(baos)) {
                    for (final SinkRecord record : buffer) {
                        outputWriter.writeRecord(record, compressedStream);
                    }
                }

                storage.create(blob, baos.toByteArray());
            } catch (final Exception e) {
                throw new ConnectException(e);
            }

            buffer.clear();
        }
    }

    private String createFilename(final TopicPartition tp, final SinkRecord firstRecord) {
        Objects.requireNonNull(tp);
        Objects.requireNonNull(firstRecord);

        String result = String.format("%s%s-%d-%d",
                config.getPrefix(), tp.topic(), tp.partition(), firstRecord.kafkaOffset());
        if (config.getCompressionType() == CompressionType.GZIP) {
            result += ".gz";
        }
        return result;
    }

    private OutputStream getCompressedStream(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);

        if (config.getCompressionType() == CompressionType.GZIP) {
            return new GZIPOutputStream(outputStream);
        } else {
            return outputStream;
        }
    }

    @Override
    public void stop() {
        // Nothing to do.
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
