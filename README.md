# digital-media-object-processor
The digital media objects processor can receive digital media object from two different sources:
- Through the API as a request to register a digital media object
- Through a Kafka queue to register an annotation

## Preparation
The digital media object processes the received objects as a batch.
To ensure that there are no conflicts, we first ensure that the batch only contains unique objects.
Objects which are updated twice in one batch are republished to the queue.
After this, we will collect the digital specimen PID from the digital specimen database.
In the received data only the physical specimen id is available, so we need to collect the digital specimen id.
If no digital specimen id is available in the database, we will requeue the item as the specimen has not been processed yet.
After this, the system will evaluate if the received objects are new, updated or equal to the current objects.
To evaluate this the system takes the following steps:
- Check if the object is already in the system, based on the properties:
  - digital specimen id
  - media url  
We assume that these two properties make the digital media object unique and can be used as keys.
No single specimen may have a digital media object with the same media url.
However, the same media url can be used with a different specimen, for example when a single image show multiple specimens.

## Evaluation with existing objects
We will now create three lists:
- A list with new items when the digital media object cannot be found in the database
- A list with updated items when a digital media object is found, but it differs from the newly received object
- A list with equal items when the digital media object is found but is equal to the newly received object  
These three list are returned and processed in order

## New digital media objects
For new digital media objects, we will create a new Handle and transfer the object to a record (adding version and timestamp).
We then push the newly create records to the database to persist them.
After the insertion in the database, we bulk index them in Elasticsearch.
After successful indexing, we publish a CreateUpdateDelete message to Kafka.
The last step is to publish an event to the different requested automated annotation services.
If everything is successful, we return the created objects, this is used as response object for the web version.
### Exception handling
When the digital media object creation fails, we will roll back on several points.
If the indexing in Elasticsearch fails, we will roll back the database insert and the handle creation.
If the publishing of the CreateUpdateDelete message fails, we will also roll back the indexing.

## Updated digital media objects
For update digital media objects we check if we need to update the handle record and if so update it and increment the version.
Next we create the digital media object records where we increment the version and create a new timestamp for the version.
We persist the new digital media record to the database, where we overwrite the old data.
After successful database insert, we bulk index the digital media object, overwriting the old data.
We publish a CreateDeleteUpdate event to Kafka.
If everything was successful, we will return the updated records.
### Exception handling
When an update on the digital media object fails, we roll back on several points.
If the indexing fails, we roll back to the previous version, which means we reinsert the old version to the handle and database.
If the publishing of the CreateUpdateDelete message fails, we will also roll back the indexing and index the previous version.

## Equal digital media objects
When the stored digital media objects and the received digital media objects are equal, we will only update the `last_checked` timestamp.
We will do a batch update to the particular field with the current timestamp to indicate the object were checked and equal at this moment.
We will not return the equal objects as we didn't change the data.

## Run locally
To run the system locally, it can be run from an IDEA.
Clone the code and fill in the application properties (see below).
The application needs to store data in a database and an Elasticsearch instance.
In Kafka mode it needs a kafka cluster to connect to and receive the messages from.

## Run as Container
The application can also be run as container.
It will require the environmental values described below.
The container can be built with the Dockerfile, in the root of the project.

## Profiles
There are two profiles with which the application can be run:
### Web
`spring.profiles.active=web`  
This listens to an API which has one endpoint:
- `POST /`
  This endpoint can be used to post a digital media object event to the processing service. 
  After this it will follow the above described process.
  It will return the newly create or updated objects.  

If an exception occurs during processing, it will be published to the Kafka Dead Letter queue.
We can than later evaluate why the exception was thrown and if needed, retry the object.

### Kafka
`spring.profiles.active=web`
This will make the application listen to a specified queue and process the digital media object events from the queue.
We collect the objects in batches of between 300-500 (depending on amount in queue).
If any exception occurs we publish the event to a Dead Letter Queue where we can evaluate the failure and if needed retry the messages.

## Environmental variables
The following backend specific properties can be configured:

```
# Database properties
spring.datasource.url=# The JDBC url to the PostgreSQL database to connect with
spring.datasource.username=# The login username to use for connecting with the database
spring.datasource.password=# The login password to use for connecting with the database

#Elasticsearch properties
elasticsearch.hostname=# The hostname of the Elasticsearch cluster
elasticsearch.port=# The port of the Elasticsearch cluster
elasticsearch.index-name=# The name of the index for Elasticsearch

# Kafka properties (only necessary when the kafka profile is active)
kafka.publisher.host=# The host address of the kafka instance to which the application will publish the CreateUpdateDelete events 
kafka.consumer.host=# The host address of the kafka instance from which the application will consume the Annotation events
kafka.consumer.group=# The group name of the kafka group from which the application will consume the Annotation events
kafka.consumer.topic=# The topic name of the kafka topic from which the application will consume the Annotation events

# Keycloak properties (only necessary when the web profile is active
keycloak.auth-server-url=# Server url of the auth endpoint of Keycloak
keycloak.realm=# Keycloak realm
keycloak.resource=# Resource name of the Keycloak resource
keycloak.ssl-required=# SSL required for the communication with Keycloak
keycloak.use-resource-role-mappings=# Resource Role Mapping true/false
keycloak.principal-attribute=# Pricinpal Attribute in JWT to check
keycloak.confidential-port=# Confidentail Keycloak port
keycloak.always-refresh-token=# ALways refresh token true/false
keycloak.bearer-only=# Only use bearer token for authentication true/false
