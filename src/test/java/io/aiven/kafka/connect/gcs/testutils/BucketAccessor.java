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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;

import io.aiven.kafka.connect.common.config.CompressionType;

import com.github.luben.zstd.ZstdInputStream;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.xerial.snappy.SnappyInputStream;

public final class BucketAccessor {
    private final Storage storage;
    private final String bucketName;
    private final boolean cache;

    private List<String> blobNamesCache = null;
    private final Map<String, String> stringContentCache = new HashMap<>();
    private final Map<String, List<String>> linesCache = new HashMap<>();
    private final Map<String, List<List<String>>> decodedLinesCache = new HashMap<>();

    public BucketAccessor(final Storage storage,
                          final String bucketName,
                          final boolean cache) {
        Objects.requireNonNull(storage, "storage cannot be null");
        Objects.requireNonNull(bucketName, "bucketName cannot be null");

        this.storage = storage;
        this.bucketName = bucketName;
        this.cache = cache;
    }

    public BucketAccessor(final Storage storage,
                          final String bucketName) {
        this(storage, bucketName, false);
    }

    public final void ensureWorking() {
        if (storage.get(bucketName) == null) {
            throw new RuntimeException("Cannot access GCS bucket \"" + bucketName + "\"");
        }
    }

    public final List<String> getBlobNames() {
        if (cache) {
            if (blobNamesCache == null) {
                blobNamesCache = getBlobNames0();
            }
            return blobNamesCache;
        } else {
            return getBlobNames0();
        }
    }

    private List<String> getBlobNames0() {
        return StreamSupport.stream(storage.list(bucketName).iterateAll().spliterator(), false)
            .map(BlobInfo::getName)
            .sorted()
            .collect(Collectors.toList());
    }

    /**
     * Get blob names with the prefix.
     *
     * <p>Doesn't support caching.
     */
    public final List<String> getBlobNames(final String prefix) {
        Objects.requireNonNull(prefix, "prefix cannot be null");

        final Storage.BlobListOption blobListOption = Storage.BlobListOption.prefix(prefix);
        return StreamSupport.stream(storage.list(bucketName, blobListOption).iterateAll().spliterator(), false)
            .map(BlobInfo::getName)
            .sorted()
            .collect(Collectors.toList());
    }

    public final void clear(final String prefix) {
        Objects.requireNonNull(prefix, "prefix cannot be null");

        final Storage.BlobListOption blobListOption = Storage.BlobListOption.prefix(prefix);
        for (final Blob blob : storage.get(bucketName).list(blobListOption).iterateAll()) {
            assert blob.delete();
        }

        if (cache) {
            blobNamesCache = null;
            stringContentCache.clear();
            linesCache.clear();
            decodedLinesCache.clear();
        }
    }

    public final String readStringContent(final String blobName,
                                          final String compression) {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        if (cache) {
            return stringContentCache.computeIfAbsent(
                blobName, k -> readStringContent0(blobName, compression));
        } else {
            return readStringContent0(blobName, compression);
        }
    }

    private String readStringContent0(final String blobName,
                                      final String compression) {
        final byte[] blobBytes = storage.readAllBytes(bucketName, blobName);
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes);
             final InputStream decompressedStream = getDecompressedStream(bais, compression);
             final InputStreamReader reader = new InputStreamReader(decompressedStream, StandardCharsets.UTF_8);
             final BufferedReader bufferedReader = new BufferedReader(reader)) {
            return bufferedReader.readLine();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public final List<String> readLines(final String blobName, final String compression) {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        if (cache) {
            return linesCache.computeIfAbsent(blobName, k -> readLines0(blobName, compression));
        } else {
            return readLines0(blobName, compression);
        }
    }

    private List<String> readLines0(final String blobName, final String compression) {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        final byte[] blobBytes = storage.readAllBytes(bucketName, blobName);
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(blobBytes);
             final InputStream decompressedStream = getDecompressedStream(bais, compression);
             final InputStreamReader reader = new InputStreamReader(decompressedStream, StandardCharsets.UTF_8);
             final BufferedReader bufferedReader = new BufferedReader(reader)) {

            return bufferedReader.lines().collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream getDecompressedStream(final InputStream inputStream, final String compression)
            throws IOException {
        Objects.requireNonNull(inputStream, "inputStream cannot be null");
        Objects.requireNonNull(compression, "compression cannot be null");

        final CompressionType compressionType = CompressionType.forName(compression);
        switch (compressionType) {
            case ZSTD:
                return new ZstdInputStream(inputStream);
            case GZIP:
                return new GZIPInputStream(inputStream);
            case SNAPPY:
                return new SnappyInputStream(inputStream);
            default:
                return inputStream;
        }
    }

    public final List<List<String>> readAndDecodeLines(final String blobName,
                                                       final String compression,
                                                       final int... fieldsToDecode) {
        Objects.requireNonNull(blobName, "blobName cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        if (cache) {
            return decodedLinesCache.computeIfAbsent(
                blobName, k -> readAndDecodeLines0(blobName, compression, fieldsToDecode));
        } else {
            return readAndDecodeLines0(blobName, compression, fieldsToDecode);
        }
    }

    private List<List<String>> readAndDecodeLines0(final String blobName,
                                                   final String compression,
                                                   final int[] fieldsToDecode) {
        return readLines(blobName, compression).stream()
            .map(l -> l.split(","))
            .map(fields -> decodeRequiredFields(fields, fieldsToDecode))
            .collect(Collectors.toList());
    }

    private List<String> decodeRequiredFields(final String[] originalFields, final int[] fieldsToDecode) {
        Objects.requireNonNull(originalFields, "originalFields cannot be null");
        Objects.requireNonNull(fieldsToDecode, "fieldsToDecode cannot be null");

        final List<String> result = Arrays.asList(originalFields);
        for (final int fieldIdx : fieldsToDecode) {
            result.set(fieldIdx, b64Decode(result.get(fieldIdx)));
        }
        return result;
    }

    public List<Record> decodeToRecords(final String blobName,
                                                       final String compression) {
        return readLines(blobName, compression).stream()
                .map(l -> l.split(","))
                .map(this::decodeRequiredFieldsToRecord)
                .collect(Collectors.toList());
    }

    private Record decodeRequiredFieldsToRecord(final String[] originalFields) {
        Objects.requireNonNull(originalFields, "originalFields cannot be null");
        final String key = b64Decode(originalFields[0]);
        final String value = b64Decode(originalFields[1]);
        final Iterable<Header> headers = decodeHeaders(originalFields[4]);
        return Record.of(key, value, headers);
    }

    public Iterable<Header> decodeHeaders(final String s) {
        final ConnectHeaders connectHeaders = new ConnectHeaders();
        final String[] headers = s.split(";");
        for (final String header : headers) {
            final String[] keyValue = header.split(":");
            final String key = b64Decode(keyValue[0]);
            final ByteArrayConverter byteArrayConverter = new ByteArrayConverter();
            final byte[] value = Base64.getDecoder().decode(keyValue[1]);
            final SchemaAndValue schemaAndValue = byteArrayConverter.toConnectHeader("topic0", key, value);
            connectHeaders.add(key, schemaAndValue);
        }
        return connectHeaders;
    }

    private String b64Decode(final String value) {
        Objects.requireNonNull(value, "value cannot be null");

        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
