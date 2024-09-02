# Game Sales Application
- created Spring Boot project via https://springinitializrjava8.cc/ using Spring Boot 2.7.18 and Java 8
## Pre-requisites
- Java 8 installed
- MySQL database and MySQL workbench installed
- any available IDE capable to run Java applications, intelliJ preferable
- create_db_schemas.sql executed
- Postman installed and Postman template imported

## Assumptions and considerations
- `date_of_sale` is in date time ISO-8601 format (`2024-04-01T00:00:00Z`)
- generating mock randomised data csv via MySQL to produce 1 CSV file with 1 million records is accepted.
- Swagger/OpenAPI specifications accepted.

## How to run app
1. Start Spring Boot App in IDE
2. Use postman template to fire `/import` request
3. Wait for csv import to complete successfully.
4. Fire either `/getGameSales`, `/getTotalSales` requests


## Considerations
- created additional indexes for `game_no` and `date_of_sale` to speed up `/getGameSales` and `/getTotalSales` queries
- created `invalid_record` table to store error-ed records during CSV parsing and validation.
- used Aspect to simplify performance logging in terms of start and end times of each REST API call

