# Pinot Pub/Sub plugin

Apache Pinot [stream ingestion plugin](https://docs.pinot.apache.org/developers/plugin-architecture/write-custom-plugins/write-your-stream) for Cloud Pub/Sub.

This plugin only implements the `High Level` type, meaning consumes data without control over the partition. Google Pub/Sub does not defined any concept of partitions like Kafka do.

## How to use it

This plugin's JAR needs to be added in pinot `/plugins` directory.
All the classes in plugins directory are loaded at pinot's startup.

This plugin is to be used when declaring Apache Pinot table's configuration.
The properties for this plugin are to be set inside the `streamConfigs` section.
Use the streamType property to define the stream type (**i.e**: `"streamType" : "pubsub"` ).

The rest of the configuration properties for your stream should be set with the prefix `"stream.pubsub"`.

## Configuration

The following table lists the configurable parameters of datasource-importer and their default values.

| Parameter                      | Description                               |
|:-------------------------------|:------------------------------------------|
| `stream.pubsub.gcpKeyPath`     | Path to GCS service account key file      |
| `stream.pubsub.projectId`      | GCP project ID of the Pub/Sub to be read  |
| `stream.pubsub.subscriptionId` | Subscription ID of the Pub/Sub to be read |
| `stream.pubsub.topicName`      | Topic name of the Pub/Sub to be read      |
