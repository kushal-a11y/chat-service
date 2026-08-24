🎥 Streaming Service --- Live Chat Hub

A real-time live-streaming chat backend built with Spring Boot,
Apache Kafka, WebSockets/STOMP, and MySQL.

The service is designed around an asynchronous event-driven pipeline:
chat messages enter through a REST API, are published to Kafka, consumed
asynchronously, and broadcast to connected viewers through WebSockets.

📌 Project Overview

The Streaming Service (Live Chat Hub) is a backend component for a
live-streaming platform.

It solves a common live-streaming problem: handling a large number of
chat messages without making the HTTP request itself responsible for
delivering every message to every connected viewer.

Instead, the application separates ingestion, processing, and delivery:

Client
  │
  │ HTTP POST
  ▼
Spring Boot REST API
  │
  │ KafkaTemplate
  ▼
Apache Kafka
  │
  │ @KafkaListener
  ▼
Kafka Consumer
  │
  │ SimpMessagingTemplate
  ▼
WebSocket / STOMP
  │
  ▼
Connected Viewers

The streamId is used as the Kafka message key so that messages
belonging to the same live stream are routed to the same partition,
preserving their ordering within that stream.

✨ Key Features

1. Real-Time Chat Broadcasting

The application uses:

Spring WebSocket

STOMP

SockJS

SimpMessagingTemplate

After a chat event is processed by Kafka, it is broadcast to subscribers
through a destination such as:

/topic/chat/{streamId}

This allows connected frontend clients to receive messages immediately
without repeatedly polling the server.

2. Event-Driven Kafka Pipeline

Chat messages are not processed entirely inside the HTTP request.

The REST API publishes the message asynchronously to Kafka:

POST /api/chat/send
        │
        ▼
Kafka Topic
        │
        ▼
Kafka Consumer

The main Kafka topic used by the application is:

live-chat-events

This provides a clear separation between:

Message ingestion

Message processing

Real-time delivery

3. Per-Stream Message Ordering

The application uses:

streamId

as the Kafka message key.

Kafka hashes the key and uses it to determine the partition.

Therefore, messages for the same stream are routed to the same
partition.

For example:

streamId = s1

Message 1 ──┐
Message 2 ──┼──> Same Kafka partition
Message 3 ──┘

This allows messages belonging to a particular stream to maintain their
Kafka partition order.

4. Asynchronous Producer

The producer uses KafkaTemplate together with CompletableFuture.

This allows the application to publish events without unnecessarily
blocking the HTTP request thread while Kafka handles the event.

Conceptually:

HTTP Request
     │
     ▼
KafkaTemplate
     │
     └──> Kafka
            │
            └──> Consumer

5. Resilient Kafka Processing

The project includes mechanisms for handling failures in the Kafka
pipeline.

ErrorHandlingDeserializer

ErrorHandlingDeserializer is used to handle malformed serialized data
safely.

This helps prevent a malformed Kafka record from repeatedly crashing or
blocking the consumer.

Retry and Dead Letter Topic

The application uses @RetryableTopic for retrying failures during
message processing.

If a message continues to fail after the configured retries, it can be
routed to a Dead Letter Topic (DLT).

Conceptually:

Kafka Topic
     │
     ▼
Consumer
     │
     ├── Success ──────> WebSocket
     │
     └── Failure
           │
           ▼
        Retry
           │
           ├── Success ──> WebSocket
           │
           └── Failure
                 │
                 ▼
                DLT

This prevents a failed message from unnecessarily blocking the normal
processing flow.

6. Programmatic Kafka Topic Provisioning

Kafka topics can be provisioned programmatically using:

KafkaAdmin

TopicBuilder

This allows the application to explicitly configure topic properties
such as:

Number of partitions

Number of replicas

instead of relying entirely on broker-side automatic topic creation.

7. Kafka Metadata and Consumer Processing

During consumption, the application processes the ChatMessage payload
and can work with Kafka metadata such as:

Partition

Offset

Stream ID

Receive time

The consumer also logs the local IST receive time for processing
visibility.

🏗️ Architecture

End-to-End Flow

                         ┌──────────────────┐
                         │   Frontend /     │
                         │   REST Client    │
                         └────────┬─────────┘
                                  │
                                  │ POST /api/chat/send
                                  ▼
                         ┌──────────────────┐
                         │   Spring Boot   │
                         │    REST API     │
                         └────────┬─────────┘
                                  │
                                  │ KafkaTemplate
                                  ▼
                    ┌─────────────────────────────┐
                    │        Apache Kafka         │
                    │                             │
                    │    live-chat-events         │
                    │                             │
                    │  streamId → partition key  │
                    └─────────────┬───────────────┘
                                  │
                                  │ @KafkaListener
                                  ▼
                         ┌──────────────────┐
                         │ Kafka Consumer   │
                         │                  │
                         │ Deserialize      │
                         │ Process message  │
                         └────────┬─────────┘
                                  │
                                  │ SimpMessagingTemplate
                                  ▼
                         ┌──────────────────┐
                         │ WebSocket/STOMP │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │ Connected       │
                         │ Viewers         │
                         └──────────────────┘

🧰 Technology Stack

Layer                     Technology

Language                  Java 17/21
Backend                   Spring Boot 3.3.x
REST API                  Spring Web
Messaging                 Apache Kafka
Kafka Integration         Spring for Apache Kafka
Real-Time Communication   Spring WebSocket
Messaging Protocol        STOMP
Browser Compatibility     SockJS
Persistence               Spring Data JPA
Database                  MySQL
Kafka Infrastructure      Apache Kafka
Kafka Monitoring          Provectus Kafka UI
Frontend Client           Vanilla HTML / JavaScript
Containerization          Docker / Docker Compose
Environment               WSL2 on Windows

📂 Main Components

The project is organized around several responsibilities:

REST Controller
     │
     └── Receives chat requests

Kafka Producer
     │
     └── Publishes ChatMessage events

Kafka Consumer
     │
     └── Consumes and processes events

WebSocket Configuration
     │
     └── Configures STOMP/WebSocket endpoints

WebSocket Publisher
     │
     └── Broadcasts processed events

Database Layer
     │
     └── Provides MySQL persistence through JPA

Kafka Infrastructure
     │
     └── Broker, topics, partitions and monitoring

🔌 API Usage

Send a Chat Message

Endpoint

POST http://localhost:7070/api/chat/send

Content-Type

Content-Type: application/json

Request Body

{
  "streamId": "s1",
  "sender": "JohnDoe",
  "content": "Hello live stream!"
}

Processing

The request follows this path:

POST /api/chat/send
       │
       ▼
Spring Boot
       │
       ▼
KafkaTemplate
       │
       ▼
live-chat-events
       │
       ▼
@KafkaListener
       │
       ▼
WebSocket
       │
       ▼
/topic/chat/s1

🌐 WebSocket Client

A built-in vanilla HTML/JavaScript client is available for testing live
chat delivery.

Start the application and open:

http://localhost:7070/index.html

Enter a stream ID such as:

s1

and connect.

The client subscribes to:

/topic/chat/s1

When a message for s1 is successfully consumed from Kafka, the
WebSocket layer broadcasts it to connected clients.

📨 Kafka Topic

The primary Kafka topic is:

live-chat-events

The streamId is used as the Kafka message key.

Example:

Key:   s1

Value:
{
  "streamId": "s1",
  "sender": "JohnDoe",
  "content": "Hello live stream!"
}

Using the same key for messages belonging to the same stream helps Kafka
route them consistently to the same partition.

🖥️ Local Development Setup

Prerequisites

Install the following:

Java 17 or Java 21

Maven

Docker

Docker Compose

MySQL

WSL2 if using the WSL-based development environment

1. Start Kafka Infrastructure

Navigate to the directory containing docker-compose.yml.

Run:

docker compose up -d

Check running containers:

docker ps

2. Open Kafka UI

Kafka UI is available at:

http://localhost:8080

Use it to inspect:

Kafka brokers

Topics

Partitions

Messages

Consumer activity

⚙️ Application Configuration

Configure the application with the required Kafka and MySQL properties.

Kafka

For a single-broker local setup:

localhost:9092

For a multi-broker setup, the bootstrap server configuration may contain
multiple broker addresses, for example:

localhost:9092,localhost:9094,localhost:9096

MySQL

Example database URL:

jdbc:mysql://localhost:3306/streaming_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true

Configure the database credentials using the application's configuration
mechanism.

Do not commit real passwords, API keys, or other secrets to GitHub.

▶️ Running the Application

Build the project:

mvn clean install

Run Spring Boot:

mvn spring-boot:run

The application runs on:

http://localhost:7070

🧪 Testing the Complete Pipeline

Step 1 --- Start Kafka

docker compose up -d

Step 2 --- Verify Kafka UI

Open:

http://localhost:8080

Step 3 --- Start Spring Boot

mvn spring-boot:run

Step 4 --- Open the WebSocket Client

http://localhost:7070/index.html

Connect using:

Stream ID: s1

Step 5 --- Send a Chat Message

POST http://localhost:7070/api/chat/send
Content-Type: application/json

{
  "streamId": "s1",
  "sender": "JohnDoe",
  "content": "Hello live stream!"
}

Step 6 --- Observe the Event

The expected flow is:

REST API
   ↓
Kafka Producer
   ↓
live-chat-events
   ↓
Kafka Consumer
   ↓
WebSocket/STOMP
   ↓
Browser

The message should appear in the connected live-chat client.

🔁 Failure Handling

The project demonstrates several Kafka reliability concepts:

                         Kafka Event
                              │
                              ▼
                         Deserializer
                              │
                    ┌─────────┴─────────┐
                    │                   │
                 Valid               Invalid
                    │                   │
                    ▼                   ▼
                Consumer        Error Handling
                    │
                    ▼
             Business Processing
                    │
             ┌──────┴──────┐
             │             │
          Success        Failure
             │             │
             ▼             ▼
         WebSocket       Retry
                           │
                    ┌──────┴──────┐
                    │             │
                 Success        Failure
                    │             │
                    ▼             ▼
                WebSocket        DLT

This architecture allows failed messages to be isolated instead of
indefinitely blocking normal chat processing.

📊 Kafka Concepts Demonstrated

This project is intended to provide practical experience with:

Kafka producers

Kafka consumers

Kafka topics

Kafka partitions

Kafka message keys

Consumer groups

Kafka offsets

Asynchronous publishing

Message serialization/deserialization

Error handling

Retry mechanisms

Dead Letter Topics

Kafka topic provisioning

Kafka UI monitoring

Event-driven architecture

Integration of Kafka with Spring Boot

🔌 WebSocket Concepts Demonstrated

The project also demonstrates:

WebSocket communication

STOMP messaging

SockJS

STOMP destinations

Application destinations

Topic subscriptions

SimpMessagingTemplate

Real-time server-to-client messaging

The WebSocket flow is:

Browser
   │
   │ Connect
   ▼
/ws-chat
   │
   ▼
STOMP
   │
   │ Subscribe
   ▼
/topic/chat/{streamId}

💾 Database

MySQL is used as the database layer through Spring Data JPA.

Example database:

streaming_db

Example JDBC URL:

jdbc:mysql://localhost:3306/streaming_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true

The database configuration is environment-specific and credentials
should be supplied locally rather than committed to the repository.

🚀 Why This Project?

This project demonstrates how a real-time feature can be designed using
an event-driven architecture rather than a simple synchronous REST
implementation.

Instead of:

Client → REST API → Client

the system uses:

Client
  ↓
REST API
  ↓
Kafka
  ↓
Consumer
  ↓
WebSocket
  ↓
Many Connected Clients

This separation makes Kafka responsible for durable event transport and
asynchronous processing while WebSockets handle real-time delivery to
connected viewers.

🔮 Possible Future Improvements

Potential extensions for the project include:

User authentication and authorization

Rate limiting for chat messages

Chat moderation

Profanity/spam filtering

Redis caching

Multiple streaming rooms

Message persistence and chat history

Consumer lag monitoring

Kafka metrics and observability

Prometheus/Grafana integration

Horizontal scaling of consumers

Multiple Kafka brokers

Schema management

Integration tests using Testcontainers

Load testing for high-volume chat traffic

👨‍💻 Development Workflow

For team development, use feature branches instead of directly modifying
main.

Example:

main
 │
 ├── feature/kafka-producer
 ├── feature/kafka-consumer
 ├── feature/websocket-chat
 └── feature/frontend

Recommended workflow:

git switch main
git pull origin main

git switch -c feature/my-feature

# Make changes

git add .
git commit -m "Implement my feature"
git push -u origin feature/my-feature

Then create a Pull Request for integration.

The main branch should be reserved for integrated, reviewed changes.

🔐 Security Notes

Never commit:

Database passwords

API keys

Access tokens

Private keys

.env files containing secrets

Use environment variables or local configuration for sensitive values.

📄 License

Add the project's chosen license here if the repository will be
distributed publicly.

⭐ Project Summary

Streaming Service --- Live Chat Hub demonstrates a real-time,
event-driven backend using:

Spring Boot
    +
Apache Kafka
    +
WebSockets/STOMP
    +
MySQL

It combines asynchronous Kafka-based event processing with real-time
WebSocket delivery, while demonstrating Kafka partition ordering,
retries, Dead Letter Topics, error handling, and programmatic topic
management.