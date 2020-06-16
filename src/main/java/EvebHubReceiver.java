
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.policy.AddHeadersPolicy;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import java.util.concurrent.CountDownLatch;

/**
 * This sample shows how to setup an Event Processor that uses a different version of blob storage service that is
 * not directly supported by Storage Blob SDK as checkpoint store.
 *
 * The following sample can be used if the environment you are targeting supports a different version of Storage Blob
 * SDK than those typically available on Azure. For example, if you are running Event Hubs on an Azure Stack Hub version
 * 2002, the highest available version for the Storage service is version 2017-11-09. In this case, you will need to use
 * the following code to change the Storage service API version to 2017-11-09. For more information on the Azure Storage
 * service versions supported on Azure Stack Hub, please refer to
 * <a href=docs.microsoft.com/azure-stack/user/azure-stack-acs-differences>Azure Stack Hub Documentation</a>
 */
public class EvebHubReceiver {

    private static final String STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=hubdemoali;AccountKey=YE0oNCQi1mhZA8HU/eos/swf2p2Zceiq9Y7JTEYHMtJ9MdgX8YfbhB7C5h2j7f36VpexpBvGOCUv5FzohqGmdQ==;EndpointSuffix=core.windows.net";
    private static final String SAS_TOKEN = "?sv=2019-10-10&ss=b&srt=sco&sp=rwdlacx&se=2020-06-17T10:31:41Z&st=2020-06-16T02:31:41Z&spr=https&sig=W9xiYEYUKGUVRphXsKCB%2BxmMVxu%2BS4mmNlCCdHldhgg%3D";
    private static final String CONTAINER_NAME = "hub";

    private static final String EH_CONNECTION_STRING = "Endpoint=sb://demoalitest.servicebus.windows.net/;SharedAccessKeyName=receiver;SharedAccessKey=K0kPrjwNl0pYa7kEJQHEV8+YUFPNTB9sk0vL0Gipy2Y=;EntityPath=demotesting";
    private static final String CONSUMER_GROUP = "con";
    private static final String EVENT_HUB_NAME = "demotesting";
    public static final String STORAGE_SERVICE_VERSION = "2017-11-09";

    /**
     * The main method to run this sample.
     *
     * @param args Ignored input arguments.
     * @throws InterruptedException If the program is interrupted while running the processor.
     */
    public static void main(String[] args) throws InterruptedException {

        // Setup the container client by adding a header policy to specify older version of storage
        BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
            .connectionString(STORAGE_CONNECTION_STRING)
            .containerName(CONTAINER_NAME)
            .sasToken(SAS_TOKEN)
            .addPolicy(new AddHeadersPolicy(new HttpHeaders().put("x-ms-version", STORAGE_SERVICE_VERSION)))
            .buildAsyncClient();

        BlobCheckpointStore blobCheckpointStore = new BlobCheckpointStore(blobContainerAsyncClient);
        CountDownLatch countDownLatch = new CountDownLatch(3);

        // Create the event processor instance
        EventProcessorClient processor = new EventProcessorClientBuilder()
            .checkpointStore(blobCheckpointStore)
            .connectionString(EH_CONNECTION_STRING, EVENT_HUB_NAME)
            .consumerGroup(CONSUMER_GROUP)
            .processEvent(eventContext -> {
                if (eventContext.getEventData().getSequenceNumber() % 100 == 0) {
                    System.out.println(
                        String.format("Event = %s, partition = %s, seq num = %d, offset = %d",
                            eventContext.getEventData().getBodyAsString(),
                            eventContext.getPartitionContext().getPartitionId(),
                            eventContext.getEventData().getSequenceNumber(),
                            eventContext.getEventData().getOffset()));
                    eventContext.updateCheckpoint();
                }
            })
            .processError(errorContext -> {
                if (!isNumeric(errorContext.getPartitionContext().getPartitionId())) {
                    countDownLatch.countDown();
                }
                System.out.println(
                    "Error " + errorContext.getPartitionContext().getPartitionId() + " " + errorContext.getThrowable()
                        .getMessage());
            })
            .buildEventProcessorClient();

        processor.start();
        countDownLatch.await();
        processor.stop();
    }

    private static boolean isNumeric(String partitionId) {
        try {
            Integer.parseInt(partitionId);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}