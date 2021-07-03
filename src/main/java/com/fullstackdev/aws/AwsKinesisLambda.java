package com.fullstackdev.aws;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * Lambda function entry point. You can change to use other pojo type or
 * implement a different RequestHandler.
 *
 * @see <a
 *      href=https://docs.aws.amazon.com/lambda/latest/dg/java-handler.html>Lambda
 *      Java Handler</a> for more information
 */

public class AwsKinesisLambda {

	private static final Logger logger = LoggerFactory.getLogger(AwsKinesisLambda.class);

	private ObjectMapper objectMapper = new ObjectMapper();

	private EmployeeEventProcessor empEventProcessor;

	private final S3Client s3Client;

	public void setEmpEventProcessor(EmployeeEventProcessor empEventProcessor) {
		this.empEventProcessor = empEventProcessor;
	}

	public AwsKinesisLambda() {
		this.empEventProcessor = new EmployeeEventProcessor();
		objectMapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
		s3Client = DependencyFactory.s3Client();
	}

//    @Override
	public void handleRequest(KinesisEvent event, final Context context) {

		List<KinesisEvent.KinesisEventRecord> records = Optional.ofNullable(event.getRecords()).orElse(emptyList());

		List<EmployeeEvent> empEvents = records.stream().flatMap(this::convertKinesisRecordToEmployeeRecord)
				.collect(toList());

		int i = empEventProcessor.processEmployeeEvents(empEvents, s3Client);

		logger.info("No. of records processed :" + i);
	}

	private Stream<EmployeeEvent> convertKinesisRecordToEmployeeRecord(KinesisEvent.KinesisEventRecord record) {
		try {
			byte[] data = record.getKinesis().getData().array();

			return Stream.of(objectMapper.readValue(data, EmployeeEvent[].class));

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

}
