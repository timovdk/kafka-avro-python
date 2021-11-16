# CONSUMER
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.deserializing_consumer import DeserializingConsumer

if __name__ == '__main__':
    # The topic name
    topic = 'test_message'

    # Connect to the schema registry
    schema_reg = SchemaRegistryClient({'url': 'http://localhost:3502'})

    # Create avro deserializers
    # These deserializer will automatically request the AVRO schemas from the schema registry 
    # (these schemas were automatically pushed by the producer)
    key_deserializer = AvroDeserializer(schema_registry_client=schema_reg)
    value_deserializer = AvroDeserializer(schema_registry_client=schema_reg)

    # The consumer calls the deserializers and prints the deserialized message
    consumer = DeserializingConsumer({
        'bootstrap.servers': 'localhost:3501',
        'group.id': 'consumers',
        'key.deserializer': key_deserializer,
        'value.deserializer': value_deserializer
    })

    # Subscribe to 'test_message' topic
    consumer.subscribe([topic])

    total_count = 0

    # Poll loop, poll kafka broker for new messages
    while True:
        try:
            msg = consumer.poll(1.0)

        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            break
        except KeyboardInterrupt:
            break

        if msg is None:
            continue

        if msg.error():
            print("AvroConsumer error: {}".format(msg.error()))
            continue

        print(msg.value())

    consumer.close()