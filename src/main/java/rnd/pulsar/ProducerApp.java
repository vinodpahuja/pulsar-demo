package rnd.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class ProducerApp {

	public static void main(String[] args) throws PulsarClientException {

		PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

		Producer<String> producer = client.newProducer(Schema.STRING).topic("my-topic").create();
		
		producer.send("My message 0");
		producer.send("My message 1");
		
		producer.close();
		
		client.close();

	}
}
