## Infrastructure
PostgreSQL

Kafka

Keycloak

## How to run
1. Start the infrastructure using `docker compose up -d`
2. Start the application using `mvn spring-boot:run`

## Notes
The application demonstrates:
1. Oauth2 JWT authentication / authorization
2. Event sourcing
3. Cqrs pattern
4. Transactional producer and listener
   1. DB operation with event publishing
   2. Pure event publishing
   3. Event consumption with DB operation
   4. Event consumption -> api -> event publishing
   5. Event consumption -> DB operation -> event publishing
5. Retryable