package rnd.pulsar;

import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class ConsumerAsyncApp {

	public static void main(String[] args) throws PulsarClientException {

		PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

		MessageListener<byte[]> listener = (consumer, msg) -> {
			try {
				System.out.println("Message received: " + //
						msg.getSequenceId() + //
						" : " + new String(msg.getData()));
				consumer.acknowledge(msg);
			} catch (Exception e) {
				consumer.negativeAcknowledge(msg);
			}
		};

		client//
		.newConsumer()//
		.topic("my-topic")//
		.subscriptionName("my-sub")//
		.subscriptionType(SubscriptionType.Shared)//
		.messageListener(listener)//
		.subscribe();
	}

}
