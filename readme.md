# Spring Kotlin Orkes Conductor Example

This project demonstrates the integration of Spring Boot with Netflix Conductor/Orkes Conductor for workflow orchestration. It provides examples of workflow definitions, task implementations, and REST API endpoints for triggering workflows.

## Prerequisites

- Java 17 or higher
- Kotlin 1.9.25
- Spring Boot 3.3.1
- Orkes Conductor Server (running on localhost:9090)

## Features

- Workflow Management
- Task Definition and Registration
- REST API Endpoints
- Multiple Worker Implementations
- Automatic Configuration

## Configuration

The application can be configured using `application.yml`:

```yaml
spring:
    application:
        name: Spring Kotlin Conductor
server:
    port: 8081
conductor:
    threadCount: 5
    timeOut: 10_000
    server:
        url: http://localhost:9090/api/
```

## Setup

1. Ensure you have Java 17 installed
2. Make sure Orkes Conductor server is running at http://localhost:9090
3. Clone the repository
4. Build the project:
   ```bash
   ./gradlew build
   ```
5. Run the application:
   ```bash
   ./gradlew bootRun
   ```

## API Endpoints

### Hello World Workflow
```http
GET /api/v1/helloworld/{name}
```
Triggers a simple hello world workflow with the provided name.

### Movie Processing Workflow
```http
GET /api/v1/movie/{movieType}/{movieId}
```
Triggers a movie processing workflow based on the movie type and ID.

## Workers

The application includes several worker implementations:

1. **HelloWorldWorker**: Handles simple hello world tasks
2. **GenerateEpisodeArtwork**: Generates artwork for TV episodes
3. **GenerateMovieArtwork**: Generates artwork for movies
4. **SetupEpisodesWorker**: Sets up TV show episodes
5. **SetupMovie**: Sets up movie information

## Dependencies

- Spring Boot Starter Web
- Jackson Module Kotlin
- Kotlin Reflect
- Orkes Conductor Client (version 2.0.8)
- Spring Boot Starter Test (for testing)
- Kotlin Test JUnit5 (for testing)

## Building

The project uses Gradle with Kotlin DSL for build configuration. Main plugins:

- Kotlin JVM
- Kotlin Spring
- Spring Boot
- Spring Dependency Management

## Contributing

Feel free to submit issues and enhancement requests.

## License

This project is licensed under the terms of the MIT license.