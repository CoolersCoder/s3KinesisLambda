

package com.ruihu.producer;

import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.ruihu.helper.KinesisHelper;
import com.ruihu.helper.S3Helper;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private static LambdaLogger Logger = null;
    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    @Override
    public String handleRequest(S3Event S3Event, Context context) {
        S3Helper s3Helper = new S3Helper();
        KinesisHelper kinesisHelper = new KinesisHelper();
        
        List<S3EventNotificationRecord> records = S3Event.getRecords();
        Iterator<S3EventNotificationRecord> s3List = records.iterator();

        while (s3List.hasNext()) {
            try {
                S3EventNotificationRecord s3EventNotificationRecord = s3List.next();
                String bucketName = s3EventNotificationRecord.getS3().getBucket().getName();
                String fileName = s3EventNotificationRecord.getS3().getObject().getKey();

                context.getLogger().log("Bucket name:" + bucketName + "-------------" + "key name:" + fileName);
                byte[] fileBytes = s3Helper.getFile(fileName, bucketName, s3, context);

                kinesisHelper.putDateInKinesis(fileBytes, Logger);

            } catch (Throwable e) {
                Logger.log(getClass() + ": Error to pass data");
            }
        }
        return "success";

    }

}
