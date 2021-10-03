package com.pc.lambda;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.pc.lambda.model.Request;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

// https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/sqs/src/main/java/com/example/sqs/SQSExample.java

public class Handler implements RequestHandler<List<Request>, String> {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public String handleRequest(List<Request> requestJson, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("\nInput => " + gson.toJson(requestJson));
		logger.log("\nContext => " + gson.toJson(context));
		logger.log("\n\n\n");

		// 1. Parse the JSON Input
		for (Request request : requestJson) {
			/*
			 * logger.log("\nRecord Id: " + String.valueOf(request.getRecordId()));
			 * logger.log("\nSend Report: " + String.valueOf(request.isSendReport()));
			 * logger.log("\nProcess Verification -> Format Data: " +
			 * String.valueOf(request.getProcessVerification().isFormatData()));
			 * logger.log("\nProcess Verification -> Match Data: " +
			 * String.valueOf(request.getProcessVerification().isMatchData()));
			 * logger.log("\nExecution States -> Document Found: " +
			 * String.valueOf(request.getExecutionStates().isDocumentFound()));
			 * logger.log("\nExecution States -> File Downloaded: " +
			 * String.valueOf(request.getExecutionStates().isFileDownloaded()));
			 */

			int priority = request.getPriority();
			logger.log("\nPriority: " + String.valueOf(priority));
			logger.log("\n");
			
			// 2. Find out the queue to which the message need to be sent based on the priority
			String sqsQueueUrl = "https://sqs.us-east-1.amazonaws.com/052843378853/demoQueue";
			/*
			 * if (priority == 2) { sqsQueueUrl = System.getenv("two-hr-tat-queue"); } else
			 * if (priority == 6) { sqsQueueUrl = System.getenv("six-hr-tat-queue"); } else
			 * if (priority == 10) { sqsQueueUrl = System.getenv("ten-hr-tat-queue"); } else
			 * if (priority == 16) { sqsQueueUrl = System.getenv("sixteen-hr-tat-queue"); }
			 */
			
			// 3. Generate Payload for SQS
			//logger.log("\nRequest JSON: " + gson.toJson(request));
			
			// 4. Send the message to the appropriate queue.
			                  
			SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
			SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(sqsQueueUrl)
					.messageBody(gson.toJson(request)).build();
			SendMessageResponse sendMessageResponse = sqsClient.sendMessage(sendMessageRequest);
			
			logger.log("\nMessage ID: " + sendMessageResponse.messageId());
		}

		return "Processed successfully";
	}

	private static void listQueuesFilter(SqsClient sqsClient, String queueUrl) {
		// List queues with filters
		String namePrefix = "queue";
		ListQueuesRequest filterListRequest = ListQueuesRequest.builder().queueNamePrefix(namePrefix).build();

		ListQueuesResponse listQueuesFilteredResponse = sqsClient.listQueues(filterListRequest);
		System.out.println("Queue URLs with prefix: " + namePrefix);
		for (String url : listQueuesFilteredResponse.queueUrls()) {
			System.out.println(url);
		}

		System.out.println("\nSend message");

		try {
			sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("Hello world!")
					.delaySeconds(10).build());
		} catch (SqsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}
}
