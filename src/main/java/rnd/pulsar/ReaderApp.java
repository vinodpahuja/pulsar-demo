package rnd.pulsar;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

public class ReaderApp {

	public static void main(String[] args) throws PulsarClientException {

		PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();

//		MessageId id = MessageId.fromByteArray("".getBytes());
//		MessageId id = MessageId.earliest;
		MessageId id = MessageId.latest;

		Reader<?> reader = client.newReader().topic("my-topic").startMessageId(id).create();

		while (true) {
			Message<?> msg = reader.readNext();
			System.out.println("Message received: " + //
					msg.getSequenceId() + //
					" : " + new String(msg.getData()));
		}
	}
}
