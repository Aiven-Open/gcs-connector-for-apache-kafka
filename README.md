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

Sometimes—for example, on reprocessing of some data—the connector will overwrite files that are already in the bucket. You need to ensure the bucket doesn't have a retention policy that prohibits overwriting.

The following object permissions must be enabled in the bucket:
- `storage.objects.create`;
- `storage.objects.delete` (needed for overwriting).

### File name format

The connector uses the following format for output files (blobs):
`<prefix><filename>`.

`<prefix>` is the optional prefix that can be used, for example, for
subdirectories in the bucket.

`<filename>` is the file name. The connector has the configurable
template for file names. It supports placeholders with variable names:
`{{ variable_name }}`. Currently supported variables are:
- `topic` - the Kafka topic;
- `partition` - the Kafka partition;
- `start_offset` - the Kafka offset of the first record in the file;
- `key` - the Kafka key.

Only the certain combinations of variables are allowed in the file name
template (however, variables in a template can be in any order). Each
combination determines the mode of record grouping the connector will
use. Currently supported combinations of variables and the corresponding
record grouping modes are:
- `topic`, `partition`, `start_offset` - grouping by the topic and
  partition;
- `key` - grouping by the key.

If the file name template is not specified, the default value is
`{{topic}}-{{partition}}-{{start_offset}}` (+ `.gz` when compression is
enabled).

### Record grouping

Incoming records are being grouped until flushed.

#### Grouping by the topic and partition

In this mode, the connector groups records by the topic and partition.
When a file is written, a offset of the first record in it is added to
its name.

For example, let's say the template is
`{{topic}}-part{{partition}}-off{{start_offset}}`. If the connector
receives records like
```
topic:topicB partition:0 offset:0
topic:topicA partition:0 offset:0
topic:topicA partition:0 offset:1
topic:topicB partition:0 offset:1
flush
```

there will be two files `topicA-part0-off0` and `topicB-part0-off0` with
two records in each.

Each `flush` produces a new set of files. For example:

```
topic:topicA partition:0 offset:0
topic:topicA partition:0 offset:1
flush
topic:topicA partition:0 offset:2
topic:topicA partition:0 offset:3
flush
```

In this case, there will be two files `topicA-part0-off0` and
`topicA-part0-off2` with two records in each.

#### Grouping by the key

In this mode, the connector groups records by the Kafka key. It always
puts one record in a file, the latest record that arrived before a flush
for each key. Also, it overwrites files if later new records with the
same keys arrive.

This mode is good for maintaining the latest values per key as files on
GCS.

Let's say the template is `k{{key}}`. For example, when the following
records arrive
```
key:0 value:0
key:1 value:1
key:0 value:2
key:1 value:3
flush
```

there will be two files `k0` (containing value `2`) and `k1` (containing
value `3`).

After a flush, previously written files might be overwritten:
```
key:0 value:0
key:1 value:1
key:0 value:2
key:1 value:3
flush
key:0 value:4
flush
```

In this case, there will be two files `k0` (containing value `4`) and
`k1` (containing value `3`).

##### The string representation of a key

The connector in this mode uses the following algorithm to create the
string representation of a key:

1. If `key` is `null`, the string value is `"null"` (i.e., string
   literal `null`).
2. If `key` schema type is `STRING`, it's used directly.
3. Otherwise, Java `.toString()` is applied.

If keys of you records are strings, you may want to use
`org.apache.kafka.connect.storage.StringConverter` as `key.converter`.

##### Warning: Single key in different partitions

The `group by key` mode primarily targets scenarios where each key
appears in one partition only. If the same key appears in multiple
partitions the result may be unexpected.

For example:
```
topic:topicA partition:0 key:x value:aaa
topic:topicA partition:1 key:x value:bbb
flush
```
file `kx` may contain `aaa` or `bbb`, i.e. the behavior is
non-deterministic.

### Data format

Output files are text files that contain one record per line (i.e.,
they're separated by `\n`).

The connector can output the following fields from records into the
output: the key, the value, the timestamp, and the offset. (The set and the order of
these output fields is configurable.) The field values are separated by comma.

The key and the value—if they're output—are stored as binaries encoded
in [Base64](https://en.wikipedia.org/wiki/Base64).

For example, if we output `key,value,offset,timestamp`, a record line might look like:
```
a2V5,TG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFtZXQ=,1232155,1554210895
```

It is possible to control the encoding of the `value` field by setting
`format.output.fields.value.encoding` to `base64` or `none`.

If the key, the value or the timestamp is null, an empty string will be
output instead:

```
,,,1554210895
```

It is possible to control the number of records to be put in a
particular output file by setting `file.max.records`. By default, it is
`0`, which is interpreted as "unlimited".

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
# (must be set to org.apache.kafka.connect.converters.ByteArrayConverter
# or org.apache.kafka.connect.storage.StringConverter)
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter

# The value converter for this connector
# (must be set to ByteArrayConverter)
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter

# A comma-separated list of topics to use as input for this connector
# Also a regular expression version `topics.regex` is supported.
# See https://kafka.apache.org/documentation/#connect_configuring
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

# GCP credentials as a JSON object.
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

# The file name template.
# See "File name format" section.
# Optional, the default is `{{topic}}-{{partition}}-{{start_offset}}` or
# `{{topic}}-{{partition}}-{{start_offset}}.gz` if the compression is enabled.
file.name.template={{topic}}-{{partition}}-{{start_offset}}.gz
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
