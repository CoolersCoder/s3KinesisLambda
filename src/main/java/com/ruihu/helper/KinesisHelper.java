package com.ruihu.helper;

import java.nio.ByteBuffer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KinesisHelper {

    private static final String STREAM_NAME = System.getenv("STREAM_NAME");
    private static final ObjectMapper mapper = new ObjectMapper();
    private AmazonKinesis kinesis;

    public KinesisHelper() {
        kinesis = AmazonKinesisClientBuilder.defaultClient();
    };


    public void putDateInKinesis(byte[] jsonDateByte, LambdaLogger Logger)
            throws JsonProcessingException {

        DescribeStreamRequest describeStreamRequest =
                new DescribeStreamRequest().withStreamName(STREAM_NAME);
        try {
            StreamDescription streamDescription =
                    kinesis.describeStream(describeStreamRequest).getStreamDescription();
            Logger.log("Stream " + STREAM_NAME + " has a status of "
                    + streamDescription.getStreamStatus());

        } catch (ResourceNotFoundException ex) {
            Logger.log("Stream " + STREAM_NAME + " does not exist.");
        }

        long createTime = System.currentTimeMillis();
        String pKey = String.format("partitionKey-%d", createTime);


        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(STREAM_NAME);
        putRecordRequest.setData(ByteBuffer.wrap(jsonDateByte));

        putRecordRequest.setPartitionKey(pKey);
        PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
        Logger.log(">>>>>>>>>>>>>" + putRecordResult.getSequenceNumber() + ">>>"
                + putRecordRequest.getPartitionKey());

    }

}
