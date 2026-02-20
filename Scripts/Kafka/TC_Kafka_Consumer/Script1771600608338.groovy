import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
import java.util.Arrays
import java.time.Duration


Properties props = new Properties()

props.put("bootstrap.servers", "localhost:9092")
props.put("group.id", "katalon-group-" + System.currentTimeMillis())

props.put("key.deserializer", StringDeserializer.class.getName())
props.put("value.deserializer", StringDeserializer.class.getName())
props.put("auto.offset.reset", "earliest")

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)

println("Subscribing to topic...")
consumer.subscribe(Arrays.asList("test-topic"))

println("Start polling...")

ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(20))

println("Total records received: " + records.count())

for (ConsumerRecord<String, String> record : records) {
    println("Received message: " + record.value())
}

consumer.close()

println("Consumer closed.")