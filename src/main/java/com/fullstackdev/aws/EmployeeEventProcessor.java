package com.fullstackdev.aws;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;

public class EmployeeEventProcessor {

	private static final Logger logger = LoggerFactory.getLogger(EmployeeEventProcessor.class);

	public int processEmployeeEvents(List<EmployeeEvent> empEvents, S3Client s3Client) {

		for (EmployeeEvent employeeEvent : empEvents) {

			logger.info("Emp event -> Id :  " + employeeEvent.getId() + " , Name :" + employeeEvent.getName()
					+ " , Address : " + employeeEvent.getAddress() + " , Salary :" + employeeEvent.getSalary());
			

			String bucket_name = employeeEvent.getId() + "-" + employeeEvent.getName().toLowerCase();

			if (s3Client.listBuckets().buckets().stream().anyMatch(bucket -> bucket.name().equals(bucket_name))) {

				logger.info("\nCannot create the bucket. \n" + "A bucket named " + bucket_name + " already exists.");

			} else {

				try {

					createBucket(s3Client, bucket_name, Region.US_EAST_1);

					createS3Object(s3Client, employeeEvent, bucket_name);

				} catch (S3Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return empEvents.size();

	}

	private void createS3Object(S3Client s3Client, EmployeeEvent employeeEvent, String bucket_name) {
		
		PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucket_name)
				.key(bucket_name + ".csv").build();
		
		String csv = employeeEvent.getId() + "," + employeeEvent.getName() + ","
				+ employeeEvent.getAddress() + "," + employeeEvent.getSalary();

		s3Client.putObject(putObjectRequest, RequestBody.fromString(csv));
		
		logger.info("S3 object created successfully for ", employeeEvent.getName());
	}

	private void createBucket(S3Client s3Client, String bucketName, Region region) throws S3Exception {

		logger.info("\nCreating a new bucket named '%s'...\n\n", bucketName);

		S3Waiter s3Waiter = s3Client.waiter();

		CreateBucketRequest bucketRequest = CreateBucketRequest.builder().bucket(bucketName).build();

		s3Client.createBucket(bucketRequest);

		HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder().bucket(bucketName).build();

		// Wait until the bucket is created and print out the response
		WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);

		waiterResponse.matched().response().ifPresent(System.out::println);

		logger.info(bucketName + " is ready");

	}

}
