import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Sample code to demonstrate how a customer might use {@link EventProcessorClient}.
 */
public class EventProcessorClientSample {

    private static final String EH_CONNECTION_STRING = "";

    /**
     * Main method to demonstrate starting and stopping a {@link EventProcessorClient}.
     *
     * @param args The input arguments to this executable.
     * @throws Exception If there are any errors while running the {@link EventProcessorClient}.
     */
    public static void main(String[] args) throws Exception {
    	
    	 BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
 	            .connectionString("")
 	            .containerName("")
 	       //     .sasToken("?sv=2019-10-10&ss=b&srt=sco&sp=rwdlacx&se=2020-06-17T10:31:41Z&st=2020-06-16T02:31:41Z&spr=https&sig=W9xiYEYUKGUVRphXsKCB%2BxmMVxu%2BS4mmNlCCdHldhgg%3D")
 	            .buildAsyncClient();


        Logger logger = LoggerFactory.getLogger(EventProcessorClientSample.class);
        Consumer<EventContext> processEvent = eventContext -> {
        	System.out.println("****data******");
        	
        	System.out.println(eventContext.getEventData());
        	
            logger.info(
                "Processing event: Event Hub name = {}; consumer group name = {}; partition id = {}; sequence number = {}",
                eventContext.getPartitionContext().getEventHubName(),
                eventContext.getPartitionContext().getConsumerGroup(),
                eventContext.getPartitionContext().getPartitionId(),
                eventContext.getEventData().getSequenceNumber());

            eventContext.updateCheckpoint();
        };

        // This error handler logs the error that occurred and keeps the processor running. If the error occurred in
        // a specific partition and had to be closed, the ownership of the partition will be given up and will allow
        // other processors to claim ownership of the partition.
        Consumer<ErrorContext> processError = errorContext -> {
        	
        	System.out.println("there is an error");
        	System.out.println(errorContext.getPartitionContext().getEventHubName() +"****" + errorContext.getPartitionContext().getConsumerGroup() + "****"
            +   errorContext.getPartitionContext().getPartitionId() + "*****"+
            errorContext.getThrowable().getMessage());
            logger.error("Error while processing {}, {}, {}, {}", errorContext.getPartitionContext().getEventHubName(),
                errorContext.getPartitionContext().getConsumerGroup(),
                errorContext.getPartitionContext().getPartitionId(),
                errorContext.getThrowable().getMessage());
        };

        EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
            .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
            .connectionString(EH_CONNECTION_STRING)
            .processEvent(processEvent)
            .processError(processError)
            .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient));

        EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();
        System.out.println("Starting event processor");
        eventProcessorClient.start();
        eventProcessorClient.start(); // should be a no-op

        // Continue to perform other tasks while the processor is running in the background.
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));

        System.out.println("Stopping event processor");
        eventProcessorClient.stop();

        Thread.sleep(TimeUnit.SECONDS.toMillis(40));
        System.out.println("Starting a new instance of event processor");
        eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();
        eventProcessorClient.start();
        // Continue to perform other tasks while the processor is running in the background.
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        System.out.println("Stopping event processor");
        eventProcessorClient.stop();
        System.out.println("Exiting process");
    }
}