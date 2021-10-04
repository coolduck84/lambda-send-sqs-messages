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
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

// https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/sqs/src/main/java/com/example/sqs/SQSExample.java

public class Handler implements RequestHandler<List<Request>, String> {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public String handleRequest(List<Request> requestJson, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("\nInput => " + gson.toJson(requestJson));

		// 1. Parse the Input JSON request.
		for (Request request : requestJson) {

			int priority = request.getPriority();
			logger.log("\nPriority: " + String.valueOf(priority));

			// 2. Find out the SQS queue URL based on the priority.
			String sqsQueueUrl = "";
			if (priority == 2) {
				sqsQueueUrl = System.getenv("twoHrTATQueue");
			} else if (priority == 6) {
				sqsQueueUrl = System.getenv("sixHrTATQueue");
			} else if (priority == 10) {
				sqsQueueUrl = System.getenv("tenHrTATQueue");
			} else if (priority == 16) {
				sqsQueueUrl = System.getenv("sixteenHrTATQueue");
			}
			logger.log("\nSQS Queue URL: " + sqsQueueUrl);

			// 3. Send the message to the appropriate queue.
			SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
			SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(sqsQueueUrl)
					.messageGroupId(String.valueOf(priority)).messageBody(gson.toJson(request)).build();
			SendMessageResponse sendMessageResponse = sqsClient.sendMessage(sendMessageRequest);

			logger.log("\nMessage ID: " + sendMessageResponse.messageId());
		}

		return "Processed successfully";
	}
}
