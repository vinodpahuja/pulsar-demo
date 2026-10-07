# Apache Pulsar Demo

A simple Java project demonstrating basic Apache Pulsar producer and consumer patterns using the Pulsar Java client.

## Overview

This repository contains example apps for:

- Producing messages to a Pulsar topic
- Consuming messages synchronously
- Consuming messages asynchronously
- Reading messages using a Pulsar Reader

The project uses:
- Java 17
- Maven
- Apache Pulsar Java client 3.0.0

## Project Structure

```text
pulsar-demo/
├── pom.xml
├── src/
│   └── main/
│       └── java/
│           └── rnd/
│               └── pulsar/
│                   ├── ProducerApp.java
│                   ├── ConsumerSyncApp.java
│                   ├── ConsumerAsyncApp.java
│                   └── ReaderApp.java
└── README.md
```

## Prerequisites

Before running the examples, ensure you have:

- Java 17+
- Maven 3.8+
- A running Pulsar broker on localhost:6650

## Start Pulsar locally

If you do not already have a Pulsar instance running, you can start a standalone broker using Docker:

```bash
docker run -it \
  -p 6650:6650 \
  -p 8080:8080 \
  apachepulsar/pulsar:3.0.0 \
  bin/pulsar standalone
```

This starts a local standalone Pulsar cluster accessible at:

```text
pulsar://localhost:6650
```

## Build the project

```bash
mvn clean compile
```

## Run the examples

### 1. Producer

This app publishes two messages to the `my-topic` topic.

```bash
mvn exec:java -Dexec.mainClass=rnd.pulsar.ProducerApp
```

### 2. Synchronous consumer

This app subscribes to `my-topic` and reads messages one-by-one using `receive()`.

```bash
mvn exec:java -Dexec.mainClass=rnd.pulsar.ConsumerSyncApp
```

### 3. Asynchronous consumer

This app registers a `MessageListener` and processes messages asynchronously.

```bash
mvn exec:java -Dexec.mainClass=rnd.pulsar.ConsumerAsyncApp
```

### 4. Reader

This app reads messages from a given start position from the topic.

```bash
mvn exec:java -Dexec.mainClass=rnd.pulsar.ReaderApp
```

## Important notes

- All example classes connect to:
  ```text
  pulsar://localhost:6650
  ```
- The topic used across the sample is:
  ```text
  my-topic
  ```
- The subscription name used by consumer apps is:
  ```text
  my-sub
  ```

## Example classes

### ProducerApp
Sends sample messages:
- `My message 0`
- `My message 1`

### ConsumerSyncApp
Consumes messages using blocking receive calls and acknowledges them after processing.

### ConsumerAsyncApp
Consumes messages using a callback-based listener and a shared subscription.

### ReaderApp
Starts reading from the latest message position and continuously prints messages.

## Maven dependency

The project includes the Pulsar client dependency in `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-client</artifactId>
    <version>3.0.0</version>
</dependency>
```

## License

This project is intended for learning and demonstration purposes.
