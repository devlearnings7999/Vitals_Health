# Vitals Health Status Application

This application consumes vitals data from a Kafka topic, classifies the data as healthy or unhealthy, and sends it to different Kafka topics based on the classification.

## Prerequisites

-   Python 3.6+
-   Kafka broker
-   `.env` file with Kafka configuration

## Installation

1.  Clone the repository:

    ```bash
    git clone <repository_url>
    cd Vitals_Health
    ```

2.  Install the dependencies:

    ```bash
    pip install --no-cache-dir -r requirements.txt
    ```

## Configuration

1.  Create a `.env` file in the root directory of the project.
2.  Add the following environment variables to the `.env` file:

    ```
    KAFKA_BROKER=your_kafka_broker:9092
    INPUT_TOPIC=vitals_output
    HEALTHY_TOPIC=healthy_vitals
    UNHEALTHY_TOPIC=unhealthy_vitals
    GROUP_ID=vitals_health_group
    SASL_USERNAME=your_sasl_username
    SASL_PASSWORD=your_sasl_password
    SECURITY_PROTOCOL=SASL_PLAINTEXT
    SASL_MECHANISM=PLAIN
    ```

    Replace the placeholder values with your actual Kafka configuration.

## Usage

1.  Run the application:

    ```bash
    python vitals_health_status.py
    ```

## Dockerization

1.  Build the Docker image:

    ```bash
    docker build -t vitals_health .
    ```

2.  Run the Docker container:

    ```bash
    docker run -d -e KAFKA_BROKER=your_kafka_broker:9092 -e INPUT_TOPIC=vitals_output -e HEALTHY_TOPIC=healthy_vitals -e UNHEALTHY_TOPIC=unhealthy_vitals -e GROUP_ID=vitals_health_group -e SASL_USERNAME=your_sasl_username -e SASL_PASSWORD=your_sasl_password -e SECURITY_PROTOCOL=SASL_PLAINTEXT -e SASL_MECHANISM=PLAIN vitals_health
    ```

    Replace the placeholder values with your actual Kafka configuration.

## Docker Compose

```yaml
version: "3.8"
services:
  vitals_health:
    build: .
    environment:
      KAFKA_BROKER: your_kafka_broker:9092
      INPUT_TOPIC: vitals_output
      HEALTHY_TOPIC: healthy_vitals
      UNHEALTHY_TOPIC: unhealthy_vitals
      GROUP_ID: vitals_health_group
      SASL_USERNAME: your_sasl_username
      SASL_PASSWORD: your_sasl_password
      SECURITY_PROTOCOL: SASL_PLAINTEXT
      SASL_MECHANISM: PLAIN
    restart: on-failure
```

```bash
docker-compose up -d
```

Replace the placeholder values with your actual Kafka configuration.
