package com.abhi.test.ai.agent.sbus.storage;

import java.io.ByteArrayInputStream;
import java.util.UUID;
import java.util.function.Consumer;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueServiceClient;
import com.azure.storage.queue.QueueServiceClientBuilder;
import com.azure.storage.queue.models.SendMessageResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

public class TestAiWithLatestSbusSdk {
	
	private static final String connectionString = "Endpoint=sb://**.servicebus.windows.net/;SharedAccessKeyName=***;SharedAccessKey=***";
	private static final String queueName = "**";
	
	private static final String BLOB_SHARED_ACCESS_SIGNATURE_KEY = "**";
	private static final String CONTAINER_NAME = "ai-agent";
	private static final String BLOB_UPLOAD_DIR = "test-ai/";
	
	private static final String QUEUE_SHARED_ACCESS_SIGNATURE_KEY = "**";
	private static final String QUEUE_NAME = "ai-agent";
	
	private static final String MONGO_CONNECTION_STRING = "mongodb://**:**@localhost:27017/?authSource=**";
	private static final String MONGO_DB_NAME = "**-test";
	private static final String MONGO_COLLECTION_NAME = "stage1";
	
	private static final Logger logger = LoggerFactory.getLogger(TestAiWithLatestSbusSdk.class);
	

	public static void main(String[] args) {
		
		TestAiWithLatestSbusSdk aiSbusPoc = new TestAiWithLatestSbusSdk();
		aiSbusPoc.startSbusListener();
	}


	private void startSbusListener() {
		logger.info("Start registering message listener");

		ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
		                                .connectionString(connectionString)
		                                .processor()
		                                .queueName(queueName)
		                                .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
		                                .disableAutoComplete() // Make sure to explicitly opt in to manual settlement (e.g. complete, abandon).
		                                .processMessage(processMessage())
		                                .processError(processErrorMessage())
		                                .disableAutoComplete()
		                                .buildProcessorClient();

		// Starts the processor in the background and returns immediately
		processorClient.start();
	}


	private Consumer<ServiceBusErrorContext> processErrorMessage() {
		// Sample code that gets called if there's an error
		Consumer<ServiceBusErrorContext> processError = errorContext -> {
			logger.error("Error occurred while receiving message: " + errorContext.getException());
		};
		return processError;
	}


	private Consumer<ServiceBusReceivedMessageContext> processMessage() {
		// Sample code that processes a single message which is received in PeekLock mode.
		Consumer<ServiceBusReceivedMessageContext> processMessage = context -> {
		    final ServiceBusReceivedMessage message = context.getMessage();
		    
		    byte[] bytes = message.getBody().toBytes();
        	String body = new String(bytes);

        	logger.info("Received - Message ID '{}', data '{}'", message.getMessageId(), body);
        	
        	Thread t1 = new Thread(new Runnable() {
			    @Override
			    public void run() {
			    	try {
		        		uploadToBlobStorage(body);
		    			sendMsgToQueueStorage(body);
		    			countMongoDbDocuments();
			            //context.complete();
			        } catch (Exception completionError) {
			        	logger.error("Completion of the message {} failed", message.getMessageId());
			            completionError.printStackTrace();
			        }
			    }
			});  
			
        	t1.start();
        	
        	context.complete();
		};
		return processMessage;
	}
	
	private void countMongoDbDocuments() {
		logger.info("Start counting documents");
		
		MongoClient mongoClient = MongoClients.create(MONGO_CONNECTION_STRING);
		MongoCollection<Document> collection = mongoClient.getDatabase(MONGO_DB_NAME).getCollection(MONGO_COLLECTION_NAME);
		
		long countDocuments = collection.countDocuments();
		
		logger.info("Finish counting documents, total documents '{}'", countDocuments);
	}
	
	private void uploadToBlobStorage(String blobContent) {
		logger.info("Start uploading blob");
		
		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().endpoint(BLOB_SHARED_ACCESS_SIGNATURE_KEY).buildClient();
		blobServiceClient.getProperties(); //to validate connection
		
		BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(CONTAINER_NAME);
		String blobName = BLOB_UPLOAD_DIR + UUID.randomUUID() + "-ai.txt";
		BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
		
		blobClient.upload(new ByteArrayInputStream(blobContent.getBytes()));
		
		logger.info("Finish uploading blob - '{}'", blobName);
	}
	
	private void sendMsgToQueueStorage(String msgContent) {
		logger.info("Start sending msg to azure queue storage");
		
		QueueServiceClient queueServiceClient = new QueueServiceClientBuilder().endpoint(QUEUE_SHARED_ACCESS_SIGNATURE_KEY).buildClient();
		queueServiceClient.getProperties(); //to validate connection
		
		QueueClient queueClient = queueServiceClient.getQueueClient(QUEUE_NAME);
		
		SendMessageResult sendMessageResult = queueClient.sendMessage(msgContent);
		
		logger.info("Finish sending msg to azure queue storage, msgID - '{}'", sendMessageResult.getMessageId());
	}

}

