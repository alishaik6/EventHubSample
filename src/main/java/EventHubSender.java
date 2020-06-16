import com.azure.messaging.eventhubs.*;

public class EventHubSender {
    public static void main(String[] args) {
        final String connectionString = "";
        final String eventHubName = "";

        // create a producer using the namespace connection string and event hub name
        EventHubProducerClient producer = new EventHubClientBuilder()
            .connectionString(connectionString, eventHubName)
            .buildProducerClient();

        // prepare a batch of events to send to the event hub    
        EventDataBatch batch = producer.createBatch();
        batch.tryAdd(new EventData("50First event"));
        batch.tryAdd(new EventData("40 Second event"));
        batch.tryAdd(new EventData("40 Third event"));
        batch.tryAdd(new EventData("40 Fourth event"));
        batch.tryAdd(new EventData("40 Fifth event"));

        // send the batch of events to the event hub
        producer.send(batch);

        // close the producer
        producer.close();
    }
}