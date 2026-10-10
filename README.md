## Infrastructure
PostgreSQL

Kafka

Keycloak

Java v25

## How to run
1. Start the infrastructure using `docker compose up -d`
2. Start the application using `mvn spring-boot:run`

## Notes
The application demonstrates:
1. Oauth2 JWT authentication / authorization
2. Event sourcing
3. Cqrs pattern
4. JPA transaction
5. Kafka Transactional producer and listener
   1. DB operation with event publishing
   2. Event consumption -> api -> event publishing
   3. Event consumption -> DB operation -> event publishing
6. Retryable
7. Outbox pattern