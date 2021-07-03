package com.fullstackdev.aws;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.Network.NetworkImpl;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public class AwsKinesisLambdaIntegrationTest {
	
	private static final Logger logger =  LoggerFactory.getLogger(AwsKinesisLambdaIntegrationTest.class);
	
	private static Network shared = Network.SHARED;	
	
	public static String networkName = ((NetworkImpl)shared).getName();
	
	static ObjectMapper objectMapper = new ObjectMapper();
	
	@Container
	  static LocalStackContainer localStack = new LocalStackContainer(DockerImageName.parse("localstack/localstack"))
			  																				.withServices(Service.S3,Service.LAMBDA,Service.KINESIS,Service.CLOUDWATCHLOGS)
			  																				.withEnv("DEFAULT_REGION", "us-east-1")
			  																				.withNetwork(shared)
			  																				.withEnv("LAMBDA_DOCKER_NETWORK", networkName)
			  																				.withCopyFileToContainer(MountableFile.forHostPath(new File("<jar file path>/localstack-kinesis-s3-demo.jar").getPath()), "/tmp/localstack/localstack-kinesis-s3-demo.jar")			  																				
			  																				;
	
	
	@BeforeAll
	  static void beforeAll() throws IOException, InterruptedException, URISyntaxException {	

		
		ExecResult lambdaCreation = localStack.execInContainer(
														      "awslocal", "lambda", "create-function",
														      "--function-name", "localstack-kinesis-s3",
														      "--runtime", "java8",
														      "--region","us-east-1",
														      "--handler", "com.fullstackdev.aws.AwsKinesisLambda::handleRequest",
														      "--role", "arn:aws:iam::123456:role/test",
														      "--zip-file","fileb:///tmp/localstack/localstack-kinesis-s3-demo.jar",
														      "--environment", "Variables={AWS_ACCESS_KEY_ID="+localStack.getAccessKey()+",AWS_SECRET_ACCESS_KEY="+localStack.getSecretKey()+"}"
													       );
		logger.info("LambdaCreation result :", lambdaCreation.getStdout());
		logger.info("LambdaCreation error :", lambdaCreation.getStderr());
		
	  }
	
	
	
	@Test
	public void testContainer() throws UnsupportedOperationException, IOException, InterruptedException, URISyntaxException {
		logger.info(localStack.getContainerName());

		
		ExecResult createStream = localStack.execInContainer(
													      	"awslocal", "kinesis", "create-stream",
													      	"--stream-name","lambda-stream",	    
													      	"--shard-count", "3"	     
												           );
		
		logger.info("CreateStream  result :",createStream.getStdout());
		logger.info("CreateStream  error :",createStream.getStderr());
		
		
		ExecResult sourcemapping = localStack.execInContainer(
														    "awslocal", "lambda", "create-event-source-mapping",
														    "--function-name","localstack-kinesis-s3",	    
														    "--batch-size", "100",
														    "--starting-position","AT_TIMESTAMP",
														    "--starting-position-timestamp","1541139109",			     
														    "--event-source-arn","arn:aws:kinesis:us-east-1:000000000000:stream/lambda-stream"			      
			    										    );
		logger.info("source mapping result :"+sourcemapping.getStdout());
		
		logger.info("source mapping error :"+sourcemapping.getStderr());
		
		ExecResult event   =   	localStack.execInContainer(
													      "awslocal", "kinesis", "put-record",
													      "--stream-name","lambda-stream",	    
													      "--partition-key", "000",
													      "--data","[{\"id\" : 1, \"name\" : \"Suresh\" , \"address\" : \"Hyderabad\" , \"salary\": 20} , {\"id\" : 2, \"name\" : \"Alex\" , \"address\" : \"Auckland\" , \"salary\": 40}]"
				                                         );
		
		logger.info("Event :"+event.getStdout());
		logger.info("Event error :"+event.getStderr());	
		
		
		ExecResult result32 = localStack.execInContainer(
			      										"awslocal", "lambda", "list-functions"			     
			    										);
		
		logger.info("result32  :"+result32.getStdout());

		
		ExecResult logGroups = localStack.execInContainer(
			      										"awslocal", "logs", "describe-log-groups"
			    										);
		
		logger.info("logGroups  :", logGroups.getStdout());
		logger.info("logGroups Error  :",logGroups.getStderr());
		
		
		ExecResult logStreams = localStack.execInContainer(
													      "awslocal", "logs", "describe-log-streams",
													      "--region","us-east-1",
													      "--log-group-name",
													      "/aws/lambda/localstack-kinesis-s3"
														 );

		
		logger.info("logStreams :",logStreams.getStdout());
		
		logger.info("logStreams Error :",logStreams.getStderr());
		
		JsonNode jsonNode = objectMapper.readTree(logStreams.getStdout()).get("logStreams");
		
		String lambdaLogGroup = "";
		
		if (jsonNode.isArray()) {
			
		    for (final JsonNode objNode : jsonNode) {
		    	
		        logger.info(objNode.get("logStreamName").asText());
		        
		        lambdaLogGroup = objNode.get("logStreamName").asText().replace("\"", "");
		        
		        ExecResult logs = localStack.execInContainer(
														    "awslocal", "logs", "get-log-events",
														    "--region","us-east-1",
														    "--log-group-name",
														    "/aws/lambda/localstack-kinesis-s3",
														    "--log-stream-name",
														    lambdaLogGroup
					    									);
		        logger.info(logs.getStdout());
		    }
		}
		
		
		ExecResult listS3buckets = localStack.execInContainer(
			      											"awslocal", "s3api", "list-buckets"			      
			    											);
				
		logger.info(" Buckets :"+listS3buckets.getStdout());
		logger.info(" Buckets :"+listS3buckets.getStderr());
		
		List<String> expectedBucketNames = new ArrayList<>();
		expectedBucketNames.add("1-suresh");
		expectedBucketNames.add("2-alex");
		
		List<String> resultBucketNames = new ArrayList<>();
		JsonNode jsonNode2 = objectMapper.readTree(listS3buckets.getStdout()).get("Buckets");
		
		if (jsonNode2.isArray()) {
			
			 for (final JsonNode objNode : jsonNode2) {
				 String bucketname = objNode.get("Name").asText();
				 logger.info("Bucket Name :",bucketname);
				 resultBucketNames.add(bucketname);	
				 ExecResult bucketContents = localStack.execInContainer(
																		"awslocal", "s3api", "list-objects","--bucket",bucketname			      
																	   );
				 logger.info(bucketContents.getStdout());
				 logger.info(bucketContents.getStderr());
			 }
		}
		
		
		assertEquals(expectedBucketNames, resultBucketNames);
	}

}
