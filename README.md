# Aiven Kafka GCS Connector

[![Build Status](https://travis-ci.org/aiven/aiven-kafka-connect-gcs.svg?branch=master)](https://travis-ci.org/aiven/aiven-kafka-connect-gcs)

This is a sink
[Kafka Connect](https://kafka.apache.org/documentation/#connect)
connector that stores Kafka messages in a
[Google Cloud Storage (GCS)](https://cloud.google.com/storage/) bucket.

## How It Works

The connector subscribes to the specified Kafka topics and collects
messages coming in them and periodically dumps the collected data to the
specified bucket in GCS.

### File name format

The connector uses the following format for output files (blobs):
`<prefix><topic>-<partition>-<start-offset>[.gz]`, where:
- `<prefix>` is the optional prefix that can be used, for example, for
  subdirectories in the bucket;
- `<topic>` is the Kafka topic name;
- `<partition>` is the topic's partition number;
- `<start-offset>` is the Kafka offset of the first record in the file;
- `[.gz]` suffix is added when compression is enabled.

### Data format

Output files are text files that contain one record pre line (i.e.,
they're separated by `\n`).

The connector can output the following fields from records into the
output: the key, the value, the timestamp, and the offset. (The set of
these output fields is configurable.) The fields are separated by comma.

The key and the value—if they're output—are stored as binaries encoded
in [Base64](https://en.wikipedia.org/wiki/Base64).

For example, a record line might look like (for the key, the value, the
offset, and the timestamp to output):
```
a2V5,TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQ=,1232155,1554210895
```

If the key, the value or the timestamp is null, an empty string will be
output instead:

```
,,,1554210895
```

## Configuration

[Here](https://kafka.apache.org/documentation/#connect_running) you can
read about the Connect workers configuration and
[here](https://kafka.apache.org/documentation/#connect_resuming), about
the connector Configuration.

Here is an example connector configuration with descriptions:

```properties
### Standard connector configuration

## Fill in your values in these:

# Unique name for the connector.
# Attempting to register again with the same name will fail.
name=my-gcs-connector

## These must have exactly these values:

# The Java class for the connector
connector.class=io.aiven.kafka.connect.gcs.GcsSinkConnector

# The key converter for this connector
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter

# The value converter for this connector
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter

# A comma-separated list of topics to use as input for this connector
# Also a regular expression version `topics.regex` is supported.
# See https://kafka.apache.org/documentation/#connect_resuming
topics=topic1,topic2


### Connector-specific configuration
### Fill in you values

# The name of the GCS bucket to use
# Required.
gcs.bucket.name=my-gcs-bucket

## The following two options are used to specify GCP credentials.
## See the overview of GCP authentication:
##  - https://cloud.google.com/docs/authentication/
##  - https://cloud.google.com/docs/authentication/production
## If they both are not present, the connector will try to detect
## the credentials automatically.
## If only one is present, the connector will use it to get the credentials.
## If both are present, this is an error.

# The path to a GCP credentials file.
# Optional, the default is null.
gcs.credentials.path=/some/path/google_credentials.json

# GCP credentials as a JSON string.
# Optional, the default is null.
gcs.credentials.json={"type":"...", ...}

##


# The set of the fields that are to be output, comma separated.
# Supported values are: `key`, `value`, `offset`, and `timestamp`.
# Optional, the default is `value`.
format.output.fields=key,value,offset,timestamp

# The prefix to be added to the name of each file put on GCS.
# See the GCS naming requirements https://cloud.google.com/storage/docs/naming
# Optional, the default is empty.
file.name.prefix=some-prefix/

# The compression type used for files put on GCS.
# The supported values are: `gzip`, `none`.
# Optional, the default is `none`.
file.compression.type=gzip
```

## Development

### Integration testing

Integration tests are implemented using JUnit, Gradle and Docker.

To run them, you need:
- a GCS bucket with the read-write permissions;
- Docker installed.

In order to run the integration tests, execute from the project root
directory:

```bash
./gradlew clean integrationTest -PtestGcsBucket=test-bucket-name
```

where `PtestGcsBucket` is the name of the GCS bucket to use.

The default GCP credentials will be used during the test (see [the GCP
documentation](https://cloud.google.com/docs/authentication/getting-started)
and
[the comment in GCP SDK code](https://github.com/googleapis/google-auth-library-java/blob/6698b3f6b5ab6017e28f68971406ca765807e169/oauth2_http/java/com/google/auth/oauth2/GoogleCredentials.java#L68)).
This can be overridden either by seting the path to the GCP credentials
file or by setting the credentials JSON string explicitly. (See
[Configuration section](#configuration) for details). 

To specify the GCS credentials path, use `gcsCredentialsPath` property:

```bash
./gradlew clean integrationTest -PtestGcsBucket=test-bucket-name \
    -PgcsCredentialsPath=/path/to/credentials.json
```

To specify the GCS credentials JSON, use `gcsCredentialsJson` property:

```bash
./gradlew clean integrationTest -PtestGcsBucket=test-bucket-name \
    -PgcsCredentialsJson='{type":"...", ...}'
```

Gralde allows to set properties using environment variables, for
example, `ORG_GRADLE_PROJECT_testGcsBucket=test-bucket-name`. See more
about the ways to set properties
[here](https://docs.gradle.org/current/userguide/build_environment.html#sec:project_properties).

### Releasing

TBD

## License

This project is licensed under the
[GNU Affero General Public License Version 3](LICENSE).
