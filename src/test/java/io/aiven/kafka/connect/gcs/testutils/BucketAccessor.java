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

package io.aiven.kafka.connect.gcs.testutils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class BucketAccessor {
    private final Storage storage;
    private final String bucketName;

    public BucketAccessor(final Storage storage,
                          final String bucketName) {
        Objects.requireNonNull(storage);
        Objects.requireNonNull(bucketName);

        this.storage = storage;
        this.bucketName = bucketName;
    }

    public final void ensureWorking() {
        if (storage.get(bucketName) == null) {
            throw new RuntimeException("Cannot access GCS bucket \"" + bucketName + "\"");
        }
    }

    public final List<String> getBlobNames() {
        return StreamSupport.stream(storage.list(bucketName).iterateAll().spliterator(), false)
                .map(BlobInfo::getName)
                .sorted()
                .collect(Collectors.toList());
    }

    public final void clear(final String prefix) {
        Objects.requireNonNull(prefix);

        final Storage.BlobListOption blobListOption = Storage.BlobListOption.prefix(prefix);
        for (final Blob blob : storage.get(bucketName).list(blobListOption).iterateAll()) {
            assert blob.delete();
        }
    }
}
