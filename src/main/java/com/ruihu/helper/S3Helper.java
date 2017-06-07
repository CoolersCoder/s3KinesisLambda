package com.ruihu.helper;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Helper {
    /**
     * 
     * @param keyName original file name
     * @param bucketName original bucket name
     * @param s3Client
     * @param context
     * @return
     */
    public byte[] getFile(String keyName, String bucketName, AmazonS3 s3Client, Context context) {
        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
        int BUFFER_SIZE = 2 * 1024;
        if (object != null) {
            InputStream in = object.getObjectContent();
            ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE);
            BufferedInputStream bis = new BufferedInputStream(in);
            byte[] buffer = new byte[BUFFER_SIZE];
            try {
                while (true) {
                    int bytesRead = bis.read(buffer);
                    if (bytesRead == -1) {
                        break;
                    } else {
                        out.write(buffer, 0, bytesRead);
                    }
                }
                return out.toByteArray();

            } catch (Exception e) {
                context.getLogger().log(e.getMessage());
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e1) {
                        context.getLogger().log(e1.getMessage());
                    }
                }

                if (bis != null) {
                    try {
                        bis.close();
                    } catch (IOException e1) {
                        context.getLogger().log(e1.getMessage());
                    }
                }
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e1) {
                        context.getLogger().log(e1.getMessage());
                    }
                }
            }

        }
        return null;
    }

    /**
     * @param fileStream
     * @param contentLength your input file stream contentLength
     * @param keyName The file name which in your target bucket.
     * @param bucketName The target bucket
     * @param s3Client context AmazonS3
     *        {@code uploadFile(objectContent,getFile().length,"your file name", "your target bucket name", s3Client}
     * 
     */
    public void copyFileToRemote(InputStream fileStream, long contentLength, String keyName,
            String bucketName, AmazonS3 s3Client) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(contentLength);
        s3Client.putObject(new PutObjectRequest(bucketName, keyName, fileStream, objectMetadata));
    }

}
