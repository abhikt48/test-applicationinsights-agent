package com.abhi.test.ai.agent.sbus.storage;

import java.io.ByteArrayInputStream;
import java.util.Hashtable;
import java.util.UUID;
import java.util.concurrent.Executors;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueServiceClient;
import com.azure.storage.queue.QueueServiceClientBuilder;
import com.azure.storage.queue.models.SendMessageResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

public class TestAiAgentWithMultiTransports implements MessageListener, ExceptionListener{
	
	private static final String SBUS_QUEUE_NAME = "test";
	private static final String SBUS_NAME = "****";
	private static final String USERNAME = "RootManageSharedAccessKey";
	private static final String PASSWORD = "****";
	private static final String QPID_CONNECTION_FACTORY_CLASS = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";

	private static final String BLOB_SHARED_ACCESS_SIGNATURE_KEY = "https://***.blob.core.windows.net/***";
	private static final String CONTAINER_NAME = "ai-agent";
	private static final String BLOB_UPLOAD_DIR = "test-ai/";
	
	private static final String QUEUE_SHARED_ACCESS_SIGNATURE_KEY = "https://***.queue.core.windows.net/***";
	private static final String QUEUE_NAME = "ai-agent";
	
	private static final String MONGO_CONNECTION_STRING = "mongodb://***:***@localhost:27017/?authSource=DB_NAME";
	private static final String MONGO_DB_NAME = "ipt-test";
	private static final String MONGO_COLLECTION_NAME = "stage1";
	
	private static final Logger logger = LoggerFactory.getLogger(TestAiAgentWithMultiTransports.class);
	

	public static void main(String[] args) throws Exception {
		
		TestAiAgentWithMultiTransports test = new TestAiAgentWithMultiTransports();
		test.registerSbusMsgListener();
		
		//Loop for not terminating the application
		int i = 0;
		while(i > 0){
			i++;
		}
	}
	
	private void countMongoDbDocuments() {
		logger.info("Start counting documents");
		
		MongoClient mongoClient = MongoClients.create(MONGO_CONNECTION_STRING);
		MongoCollection<Document> collection = mongoClient.getDatabase(MONGO_DB_NAME).getCollection(MONGO_COLLECTION_NAME);
		
		long countDocuments = collection.countDocuments();
		
		logger.info("Finish counting documents, total documents '{}'", countDocuments);
	}
	
	private void uploadBlob(String blobContent) {
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
	
	
	private void registerSbusMsgListener() throws NamingException, JMSException, InterruptedException {
		logger.info("Start registering message listener");
		
		Hashtable<String, String> hashtable = new Hashtable<>();
		hashtable.put("connectionfactory.SBCF", "amqps://"+ SBUS_NAME +".servicebus.windows.net");
		hashtable.put(Context.INITIAL_CONTEXT_FACTORY, QPID_CONNECTION_FACTORY_CLASS);

		Context context = new InitialContext(hashtable);
		ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("SBCF");
		
		Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
		connection.setExceptionListener(this);
		connection.start();
		
		Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		
		Destination destination = session.createQueue(SBUS_QUEUE_NAME);
		
		MessageConsumer consumer = session.createConsumer(destination);
		
		consumer.setMessageListener(this);
		
		logger.info("Registered MessageListener successfully on queue '{}'", SBUS_QUEUE_NAME);
		
	}
	

	@Override
	public void onMessage(javax.jms.Message jmsMessage) {
		try {
			logger.info("Received Message '{}'", jmsMessage.getJMSMessageID());
			
			String msgBody = null;
			if (jmsMessage instanceof JmsTextMessage) {
				msgBody = ((JmsTextMessage) jmsMessage).getText();
			} else {
				BytesMessage bytesMessage = (JmsBytesMessage) jmsMessage;
				byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
				bytesMessage.readBytes(bytes);
				msgBody = new String(bytes);
			}
			
			logger.info("Received Message body '{}'", msgBody);
			
			jmsMessage.acknowledge();

			final String body = msgBody;
			
			Thread t1 = new Thread(new Runnable() {
			    @Override
			    public void run() {
			    	uploadBlob(body);
					sendMsgToQueueStorage(body);
					countMongoDbDocuments();
			    }
			});  
			t1.start();
			
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onException(JMSException exception) {
		System.err.println("*** onException :: exception message :: ***" + exception.getMessage());
		exception.printStackTrace();
		
	}
	
	
}
