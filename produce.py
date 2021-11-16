# PRODUCER
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient

if __name__ == '__main__':
    # The topic name
    topic = 'test_message'

    # Load the schemas
    schema_key = open("schemas/test_message-key.avsc").read()
    schema_value = open("schemas/test_message-value.avsc").read()

    # Connect to the schema registry
    schema_reg = SchemaRegistryClient({'url': 'http://localhost:3502'})

    # Create avro serializers
    # These serializers will automatically publish new schemas to the schema registry
    # And then serialize the message
    key_serializer = AvroSerializer(schema_registry_client=schema_reg, schema_str=schema_key)
    value_serializer = AvroSerializer(schema_registry_client=schema_reg, schema_str=schema_value)

    # The producer calls the serializers and sends the serialized message to the KAFKA broker
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:3501',
        'key.serializer': key_serializer,
        'value.serializer': value_serializer
    })

    delivered_records = 0

    # On ACK
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    # Send 10 messages to kafka
    for n in range(10):
        print("Producing Avro record: {}\t{}".format('prod_1', 'Hello World'))
        producer.produce(topic=topic, key={'sender': 'prod_1'}, value={'message': 'Hello World'}, on_delivery=acked)
        producer.poll(0)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))