import com.azure.messaging.eventhubs.*;

public class EventHubSender {
    public static void main(String[] args) {
        final String connectionString = "Endpoint=sb://demoalitest.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=XbLxpX89i3kMa4SysLHzNyJm9pnNEEgph7i9VLn5pdQ=;EntityPath=demotesting";
        final String eventHubName = "demotesting";

        // create a producer using the namespace connection string and event hub name
        EventHubProducerClient producer = new EventHubClientBuilder()
            .connectionString(connectionString, eventHubName)
            .buildProducerClient();

        // prepare a batch of events to send to the event hub    
        EventDataBatch batch = producer.createBatch();
        batch.tryAdd(new EventData("10First event"));
        batch.tryAdd(new EventData("20 Second event"));
        batch.tryAdd(new EventData("30 Third event"));
        batch.tryAdd(new EventData("40 Fourth event"));
        batch.tryAdd(new EventData("50 Fifth event"));

        // send the batch of events to the event hub
        producer.send(batch);

        // close the producer
        producer.close();
    }
}