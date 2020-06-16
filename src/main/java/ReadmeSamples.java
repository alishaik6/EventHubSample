import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import java.util.concurrent.TimeUnit;

/**
 * WARNING: MODIFYING THIS FILE WILL REQUIRE CORRESPONDING UPDATES TO README.md FILE. LINE NUMBERS ARE USED TO EXTRACT
 * APPROPRIATE CODE SEGMENTS FROM THIS FILE. ADD NEW CODE AT THE BOTTOM TO AVOID CHANGING LINE NUMBERS OF EXISTING CODE
 * SAMPLES.
 *
 * Class containing code snippets that will be injected to README.md.
 */
public class ReadmeSamples {
	
	  public static void main(String[] args) {
		  
		  ReadmeSamples oReadmeSamples = new ReadmeSamples();
		  try {
			oReadmeSamples.consumeEventsUsingEventProcessor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }

    /**
     * Code sample for creating an async blob container client.
     */
    public void createBlobContainerClient() {
        BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
            .connectionString("DefaultEndpointsProtocol=https;AccountName=hubdemoali;AccountKey=YE0oNCQi1mhZA8HU/eos/swf2p2Zceiq9Y7JTEYHMtJ9MdgX8YfbhB7C5h2j7f36VpexpBvGOCUv5FzohqGmdQ==;EndpointSuffix=core.windows.net")
            .containerName("hub")
            .sasToken("?sv=2019-10-10&ss=b&srt=sco&sp=rwdlacx&se=2020-06-17T10:31:41Z&st=2020-06-16T02:31:41Z&spr=https&sig=W9xiYEYUKGUVRphXsKCB%2BxmMVxu%2BS4mmNlCCdHldhgg%3D")
            .buildAsyncClient();
    }

    /**
     * Code sample for consuming events from event processor with blob checkpoint store.
     * @throws InterruptedException If the thread is interrupted.
     */
    public void consumeEventsUsingEventProcessor() throws InterruptedException {
    	 BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
    	            .connectionString("DefaultEndpointsProtocol=https;AccountName=hubdemoali;AccountKey=YE0oNCQi1mhZA8HU/eos/swf2p2Zceiq9Y7JTEYHMtJ9MdgX8YfbhB7C5h2j7f36VpexpBvGOCUv5FzohqGmdQ==;EndpointSuffix=core.windows.net")
    	            .containerName("hub")
    	            .sasToken("?sv=2019-10-10&ss=b&srt=sco&sp=rwdlacx&se=2020-06-17T10:31:41Z&st=2020-06-16T02:31:41Z&spr=https&sig=W9xiYEYUKGUVRphXsKCB%2BxmMVxu%2BS4mmNlCCdHldhgg%3D")
    	            .buildAsyncClient();

        EventProcessorClient eventProcessorClient = new EventProcessorClientBuilder()
            .consumerGroup("con")
            .connectionString("Endpoint=sb://demoalitest.servicebus.windows.net/;SharedAccessKeyName=receiver;SharedAccessKey=K0kPrjwNl0pYa7kEJQHEV8+YUFPNTB9sk0vL0Gipy2Y=;EntityPath=demotesting")
            .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient))
            .processEvent(eventContext -> {
                System.out.println("Partition id = " + eventContext.getPartitionContext().getPartitionId() + " and "
                    + "sequence number of event = " + eventContext.getEventData().getSequenceNumber());
            })
            .processError(errorContext -> {
                System.out.println("Error occurred while processing events " + errorContext.getThrowable().getMessage());
            })
            .buildEventProcessorClient();

        // This will start the processor. It will start processing events from all partitions.
        eventProcessorClient.start();

        // (for demo purposes only - adding sleep to wait for receiving events)
        TimeUnit.SECONDS.sleep(2);

        // When the user wishes to stop processing events, they can call `stop()`.
        eventProcessorClient.stop();
    }
}
