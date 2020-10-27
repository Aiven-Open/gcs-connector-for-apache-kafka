/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.gcs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonLinesOutputWriter;
import io.aiven.kafka.connect.common.output.jsonwriter.JsonOutputWriter;
import io.aiven.kafka.connect.common.output.plainwriter.PlainOutputWriter;

import com.github.luben.zstd.ZstdOutputStream;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

public final class GcsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(GcsSinkConnector.class);

    private RecordGrouper recordGrouper;

    private GcsSinkConfig config;

    private Storage storage;

    // required by Connect
    public GcsSinkTask() {
    }

    // for testing
    public GcsSinkTask(final Map<String, String> props,
                          final Storage storage) {
        Objects.requireNonNull(props, "props cannot be null");
        Objects.requireNonNull(storage, "storage cannot be null");

        this.config = new GcsSinkConfig(props);
        this.storage = storage;
        initRest();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props cannot be null");

        this.config = new GcsSinkConfig(props);
        this.storage = StorageOptions.newBuilder()
            .setCredentials(config.getCredentials())
            .build()
            .getService();
        initRest();
    }

    private void initRest() {
        try {
            this.recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        } catch (final Exception e) {
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
    }

    private OutputWriter getOutputWriter(final OutputStream outputStream) {
        switch (this.config.getFormatType()) {
            case CSV:
                return new PlainOutputWriter(config.getOutputFields(), outputStream);
            case JSONL:
                return new JsonLinesOutputWriter(config.getOutputFields(), outputStream);
            case JSON:
                return new JsonOutputWriter(config.getOutputFields(), outputStream);
            default:
                throw new ConnectException("Unsupported format type " + config.getFormatType());
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");

        log.debug("Processing {} records", records.size());
        for (final SinkRecord record : records) {
            recordGrouper.put(record);
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        recordGrouper.records().forEach(this::flushFile);
        recordGrouper.clear();
    }

    private void flushFile(final String filename, final List<SinkRecord> records) {
        final BlobInfo blob = BlobInfo
            .newBuilder(config.getBucketName(), config.getPrefix() + filename)
            .build();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // Don't group these two tries,
            // because the internal one must be closed before writing to GCS.
            try (final OutputStream compressedStream = getCompressedStream(baos)) {
                try (final OutputWriter outputWriter = getOutputWriter(compressedStream)) {
                    for (final SinkRecord record: records) {
                        outputWriter.writeRecord(record);
                    }
                }
            }
            storage.create(blob, baos.toByteArray());
        } catch (final Exception e) {
            throw new ConnectException(e);
        }
    }

    private OutputStream getCompressedStream(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream, "outputStream cannot be null");

        switch (config.getCompressionType()) {
            case ZSTD:
                return new ZstdOutputStream(outputStream);
            case GZIP:
                return new GZIPOutputStream(outputStream);
            case SNAPPY:
                return new SnappyOutputStream(outputStream);
            default:
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
