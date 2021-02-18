# Deprecation

We initally wrote this plugin to ingest data from GCP's Pubsub into Pinot, however we find out that it was impossible to do it reliably.
You can find a post mortem here: https://github.com/apache/incubator-pinot/issues/6556

# Pinot Pub/Sub plugin

Apache Pinot [stream ingestion plugin](https://docs.pinot.apache.org/developers/plugin-architecture/write-custom-plugins/write-your-stream) for Cloud Pub/Sub.

This plugin only implements the `LowLevel` consumer type even though PubSub doesn't have a concept of partition (which is how low level ingestion is supposed to work). We are forced to hack around this by providing arbitrary offset so we can ingest events as pinot expect.

## How to use it

This plugin's JAR needs to be added in pinot `/plugins` directory.
All the classes in plugins directory are loaded at pinot's startup.

This plugin is to be used when declaring Apache Pinot table's configuration.
The properties for this plugin are to be set inside the `streamConfigs` section.
Use the streamType property to define the stream type (**i.e**: `"streamType" : "pubsub"` ).

The rest of the configuration properties for your stream should be set with the prefix `"stream.pubsub"`.

## Configuration

Here is a list of available configurations for this plugin.
Remember, due to the lack of low level stream type, some configurations have to be set with default values (See below):

| Parameter                                   | Description                                    |
|:--------------------------------------------|:-----------------------------------------------|
| `stream.pubsub.projectId`                   | GCP project ID of the Pub/Sub to be read       |
| `stream.pubsub.subscriptionId`              | Subscription ID of the Pub/Sub to be read      |
| `stream.pubsub.topicName`                   | Topic name of the Pub/Sub to be read           |
