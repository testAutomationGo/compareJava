package com.testsuite.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.BucketLifecycleConfiguration;
import software.amazon.awssdk.services.s3.model.BucketLoggingStatus;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CORSConfiguration;
import software.amazon.awssdk.services.s3.model.CORSRule;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketLifecycleResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ErrorDocument;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLoggingRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLoggingResponse;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.GetBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetBucketTaggingResponse;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.GetBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.GetBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.GetPublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.GetPublicAccessBlockResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.IndexDocument;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.LoggingEnabled;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectOwnership;
import software.amazon.awssdk.services.s3.model.OwnershipControls;
import software.amazon.awssdk.services.s3.model.OwnershipControlsRule;
import software.amazon.awssdk.services.s3.model.PublicAccessBlockConfiguration;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAclResponse;
import software.amazon.awssdk.services.s3.model.PutBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutBucketLoggingRequest;
import software.amazon.awssdk.services.s3.model.PutBucketLoggingResponse;
import software.amazon.awssdk.services.s3.model.PutBucketOwnershipControlsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketOwnershipControlsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyResponse;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.PutBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.PutBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.PutPublicAccessBlockRequest;
import software.amazon.awssdk.services.s3.model.PutPublicAccessBlockResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionByDefault;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionConfiguration;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionRule;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;
import software.amazon.awssdk.services.s3.model.WebsiteConfiguration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

public class S3Utilities {
    private final S3Client s3;
    private final S3Presigner presigner;

    public S3Utilities(Region region, String awsKey, String awsSecret) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(awsKey, awsSecret);
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(awsCreds);
        
        this.s3 = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();
                
        this.presigner = S3Presigner.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();
    }

    public CreateBucketResponse createBucket(String bucket) {
        return s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    }

    public PutObjectResponse putObject(String bucket, String key, byte[] content, String contentType) {
        return s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType).build(),
                RequestBody.fromBytes(content)
        );
    }

    public PutObjectResponse putObjectWithMetadata(String bucket, String key, byte[] content, String contentType, Map<String, String> metadata) {
        return s3.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType(contentType)
                    .metadata(metadata)
                    .build(),
                RequestBody.fromBytes(content)
        );
    }

    public PutObjectResponse putObjectWithStorageClass(String bucket, String key, byte[] content, String contentType, StorageClass storageClass) {
        return s3.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType(contentType)
                    .storageClass(storageClass)
                    .build(),
                RequestBody.fromBytes(content)
        );
    }

    public PutObjectResponse putObjectWithSSE(String bucket, String key, byte[] content, String contentType) {
        return s3.putObject(
                PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType(contentType)
                    .serverSideEncryption(ServerSideEncryption.AES256)
                    .build(),
                RequestBody.fromBytes(content)
        );
    }

    public PutObjectResponse putFileObject(String bucket, String key, Path file, String contentType) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        return s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType).build(),
                RequestBody.fromBytes(bytes)
        );
    }

    public HeadBucketResponse headBucket(String bucket) {
        return s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
    }

    public HeadObjectResponse headObject(String bucket, String key) {
        return s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
    }

    public DeleteObjectResponse deleteObject(String bucket, String key) {
        return s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
    }

    public void emptyBucket(String bucket) {
        String token = null;
        do {
            ListObjectsV2Response page = s3.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(bucket).continuationToken(token).build()
            );
            for (S3Object obj : page.contents()) {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(obj.key()).build());
            }
            token = page.nextContinuationToken();
        } while (token != null);
    }

    public DeleteBucketResponse deleteBucket(String bucket) {
        return s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
    }

    public ListBucketsResponse listBuckets() {
        return s3.listBuckets();
    }

    public ListObjectsResponse listObjects(String bucket) {
        return s3.listObjects(ListObjectsRequest.builder().bucket(bucket).build());
    }

    public ListObjectsV2Response listObjectsV2(String bucket) {
        return s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).build());
    }

    public ListObjectsV2Response listObjectsWithPrefix(String bucket, String prefix) {
        return s3.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build());
    }

    public ListObjectsV2Response listObjectsWithDelimiter(String bucket, String prefix, String delimiter) {
        return s3.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .delimiter(delimiter)
                .build());
    }

    public ResponseInputStream<GetObjectResponse> getObjectAsStream(String bucket, String key) {
        return s3.getObject(
            GetObjectRequest.builder().bucket(bucket).key(key).build()
        );
    }

    public ResponseInputStream<GetObjectResponse> getObjectRange(String bucket, String key, long start, long end) {
        return s3.getObject(
            GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=" + start + "-" + end)
                .build()
        );
    }

    public PutBucketVersioningResponse putBucketVersioning(String bucket, BucketVersioningStatus status) {
        return s3.putBucketVersioning(PutBucketVersioningRequest.builder()
                .bucket(bucket)
                .versioningConfiguration(VersioningConfiguration.builder()
                        .status(status)
                        .build())
                .build());
    }

    public GetBucketVersioningResponse getBucketVersioning(String bucket) {
        return s3.getBucketVersioning(GetBucketVersioningRequest.builder()
                .bucket(bucket)
                .build());
    }

    public ListObjectVersionsResponse listObjectVersions(String bucket, String prefix) {
        return s3.listObjectVersions(ListObjectVersionsRequest.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build());
    }

    public PutPublicAccessBlockResponse putPublicAccessBlock(String bucket, PublicAccessBlockConfiguration config) {
        return s3.putPublicAccessBlock(PutPublicAccessBlockRequest.builder()
                .bucket(bucket)
                .publicAccessBlockConfiguration(config)
                .build());
    }

    public GetPublicAccessBlockResponse getPublicAccessBlock(String bucket) {
        return s3.getPublicAccessBlock(GetPublicAccessBlockRequest.builder()
                .bucket(bucket)
                .build());
    }

    public PutBucketEncryptionResponse putBucketEncryption(String bucket) {
        return s3.putBucketEncryption(PutBucketEncryptionRequest.builder()
                .bucket(bucket)
                .serverSideEncryptionConfiguration(ServerSideEncryptionConfiguration.builder()
                        .rules(ServerSideEncryptionRule.builder()
                                .applyServerSideEncryptionByDefault(ServerSideEncryptionByDefault.builder()
                                        .sseAlgorithm(ServerSideEncryption.AES256)
                                        .build())
                                .build())
                        .build())
                .build());
    }

    public GetBucketEncryptionResponse getBucketEncryption(String bucket) {
        return s3.getBucketEncryption(GetBucketEncryptionRequest.builder()
                .bucket(bucket)
                .build());
    }

    public PutBucketCorsResponse putBucketCors(String bucket, String origin) {
        CORSRule corsRule = CORSRule.builder()
                .allowedHeaders("*")
                .allowedMethods("GET")
                .allowedOrigins(origin)
                .exposeHeaders("ETag")
                .maxAgeSeconds(300)
                .build();

        return s3.putBucketCors(PutBucketCorsRequest.builder()
                .bucket(bucket)
                .corsConfiguration(CORSConfiguration.builder()
                        .corsRules(corsRule)
                        .build())
                .build());
    }

    public GetBucketCorsResponse getBucketCors(String bucket) {
        return s3.getBucketCors(GetBucketCorsRequest.builder()
                .bucket(bucket)
                .build());
    }

    public PutBucketWebsiteResponse putBucketWebsite(String bucket, String indexDocument, String errorDocument) {
        WebsiteConfiguration.Builder configBuilder = WebsiteConfiguration.builder()
                .indexDocument(IndexDocument.builder().suffix(indexDocument).build());
        
        if (errorDocument != null && !errorDocument.isEmpty()) {
            configBuilder.errorDocument(ErrorDocument.builder().key(errorDocument).build());
        }

        return s3.putBucketWebsite(PutBucketWebsiteRequest.builder()
                .bucket(bucket)
                .websiteConfiguration(configBuilder.build())
                .build());
    }

    public GetBucketWebsiteResponse getBucketWebsite(String bucket) {
        return s3.getBucketWebsite(GetBucketWebsiteRequest.builder()
                .bucket(bucket)
                .build());
    }

    public DeleteBucketWebsiteResponse deleteBucketWebsite(String bucket) {
        return s3.deleteBucketWebsite(DeleteBucketWebsiteRequest.builder()
                .bucket(bucket)
                .build());
    }

    public PutBucketLifecycleConfigurationResponse putBucketLifecycle(String bucket, String prefix, int expirationDays) {
        LifecycleRule rule = LifecycleRule.builder()
                .id("TestRule")
                .status(ExpirationStatus.ENABLED)
                .filter(LifecycleRuleFilter.builder()
                        .prefix(prefix)
                        .build())
                .expiration(LifecycleExpiration.builder()
                        .days(expirationDays)
                        .build())
                .build();

        return s3.putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest.builder()
                .bucket(bucket)
                .lifecycleConfiguration(BucketLifecycleConfiguration.builder()
                        .rules(rule)
                        .build())
                .build());
    }

    public GetBucketLifecycleConfigurationResponse getBucketLifecycle(String bucket) {
        return s3.getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest.builder()
                .bucket(bucket)
                .build());
    }

    public DeleteBucketLifecycleResponse deleteBucketLifecycle(String bucket) {
        return s3.deleteBucketLifecycle(DeleteBucketLifecycleRequest.builder()
                .bucket(bucket)
                .build());
    }

    public PutBucketTaggingResponse putBucketTagging(String bucket, Map<String, String> tags) {
        List<Tag> tagList = tags.entrySet().stream()
                .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                .collect(Collectors.toList());

        return s3.putBucketTagging(PutBucketTaggingRequest.builder()
                .bucket(bucket)
                .tagging(Tagging.builder().tagSet(tagList).build())
                .build());
    }

    public GetBucketTaggingResponse getBucketTagging(String bucket) {
        return s3.getBucketTagging(GetBucketTaggingRequest.builder()
                .bucket(bucket)
                .build());
    }

    public PutObjectTaggingResponse putObjectTagging(String bucket, String key, Map<String, String> tags) {
        List<Tag> tagList = tags.entrySet().stream()
                .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                .collect(Collectors.toList());

        return s3.putObjectTagging(PutObjectTaggingRequest.builder()
                .bucket(bucket)
                .key(key)
                .tagging(Tagging.builder().tagSet(tagList).build())
                .build());
    }

    public GetObjectTaggingResponse getObjectTagging(String bucket, String key) {
        return s3.getObjectTagging(GetObjectTaggingRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
    }

    public DeleteObjectTaggingResponse deleteObjectTagging(String bucket, String key) {
        return s3.deleteObjectTagging(DeleteObjectTaggingRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
    }

    public CreateMultipartUploadResponse createMultipartUpload(String bucket, String key) {
        return s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
    }

    public UploadPartResponse uploadPart(String bucket, String key, String uploadId, int partNumber, byte[] data) {
        return s3.uploadPart(UploadPartRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build(),
                RequestBody.fromBytes(data));
    }

    public CompleteMultipartUploadResponse completeMultipartUpload(String bucket, String key, String uploadId, List<CompletedPart> parts) {
        return s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(parts)
                        .build())
                .build());
    }

    public AbortMultipartUploadResponse abortMultipartUpload(String bucket, String key, String uploadId) {
        return s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .build());
    }

    public ListMultipartUploadsResponse listMultipartUploads(String bucket) {
        return s3.listMultipartUploads(ListMultipartUploadsRequest.builder()
                .bucket(bucket)
                .build());
    }

    public CopyObjectResponse copyObject(String sourceBucket, String sourceKey, String destBucket, String destKey) {
        return s3.copyObject(CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(destBucket)
                .destinationKey(destKey)
                .build());
    }

    public CopyObjectResponse copyObjectWithMetadata(String sourceBucket, String sourceKey, String destBucket, String destKey, Map<String, String> metadata) {
        return s3.copyObject(CopyObjectRequest.builder()
                .sourceBucket(sourceBucket)
                .sourceKey(sourceKey)
                .destinationBucket(destBucket)
                .destinationKey(destKey)
                .metadataDirective(MetadataDirective.REPLACE)
                .metadata(metadata)
                .build());
    }

    public String generatePresignedGetUrl(String bucket, String key, Duration expiration) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(expiration)
                .getObjectRequest(getObjectRequest)
                .build();

        return presigner.presignGetObject(presignRequest).url().toString();
    }

    public String generatePresignedPutUrl(String bucket, String key, Duration expiration) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                .signatureDuration(expiration)
                .putObjectRequest(putObjectRequest)
                .build();

        return presigner.presignPutObject(presignRequest).url().toString();
    }

    public String generateRandomBucketName() {
        return "bucket-" + System.currentTimeMillis() + "-" + (int)(Math.random() * 10000);
    }

    public void waitForBucketExists(String bucket) {
        long[] delays = {150, 300, 600, 1000};
        long startTime = System.currentTimeMillis();
        long timeout = 3000;
        int i = 0;
        while (System.currentTimeMillis() - startTime < timeout) {
            try {
                headBucket(bucket);
                return;
            } catch (Exception ignored) {}
            long delay = (i < delays.length) ? delays[i] : delays[delays.length - 1];
            long end = System.currentTimeMillis() + delay;
            while (System.currentTimeMillis() < end) {
            }
            i++;
        }
    }

    public PutBucketPolicyResponse putBucketPolicy(String bucketName, String policy) {
        PutBucketPolicyRequest request = PutBucketPolicyRequest.builder()
                .bucket(bucketName)
                .policy(policy)
                .build();
        return s3.putBucketPolicy(request);
    }

    public GetBucketPolicyResponse getBucketPolicy(String bucketName) {
        GetBucketPolicyRequest request = GetBucketPolicyRequest.builder()
                .bucket(bucketName)
                .build();
        return s3.getBucketPolicy(request);
    }

    public PutBucketAclResponse putBucketAcl(String bucketName, String acl) {
        BucketCannedACL cannedAcl = BucketCannedACL.fromValue(acl);
        PutBucketAclRequest request = PutBucketAclRequest.builder()
                .bucket(bucketName)
                .acl(cannedAcl)
                .build();
        return s3.putBucketAcl(request);
    }

    public GetBucketAclResponse getBucketAcl(String bucketName) {
        GetBucketAclRequest request = GetBucketAclRequest.builder()
                .bucket(bucketName)
                .build();
        return s3.getBucketAcl(request);
    }

    public void close() {
        if (s3 != null) {
            s3.close();
        }
        if (presigner != null) {
            presigner.close();
        }
    }

    public PutBucketLoggingResponse putBucketLogging(String bucketName, String targetBucketName, String logPrefix) {
        LoggingEnabled loggingEnabled = LoggingEnabled.builder()
                .targetBucket(targetBucketName)
                .targetPrefix(logPrefix)
                .build();
        BucketLoggingStatus loggingStatus = BucketLoggingStatus.builder()
                .loggingEnabled(loggingEnabled)
                .build();
        PutBucketLoggingRequest request = PutBucketLoggingRequest.builder()
                .bucket(bucketName)
                .bucketLoggingStatus(loggingStatus)
                .build();
        return s3.putBucketLogging(request);
    }

    public GetBucketLoggingResponse getBucketLogging(String bucketName) {
        GetBucketLoggingRequest request = GetBucketLoggingRequest.builder()
                .bucket(bucketName)
                .build();
        return s3.getBucketLogging(request);
    }

    public GetObjectResponse getObjectVersion(String bucketName, String objectKey, String versionId) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .versionId(versionId)
                .build();
        return s3.getObject(request).response();
    }

    public software.amazon.awssdk.services.s3.model.DeleteObjectResponse deleteObjectVersion(String bucketName, String objectKey, String versionId) {
        var deleteReq = software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(objectKey)
            .versionId(versionId)
            .build();
        return s3.deleteObject(deleteReq);
    }

    public ListPartsResponse listParts(String bucketName, String objectKey, String uploadId) {
        ListPartsRequest request = ListPartsRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .uploadId(uploadId)
                .build();
        return s3.listParts(request);
    }

 

    public PutBucketOwnershipControlsResponse putBucketOwnershipControls(String bucket, String ownership) {
        ObjectOwnership ownershipValue = ObjectOwnership.fromValue(ownership);
        OwnershipControls controls = OwnershipControls.builder()
            .rules(OwnershipControlsRule.builder()
                    .objectOwnership(ownershipValue)
                    .build())
            .build();
    
        return s3.putBucketOwnershipControls(PutBucketOwnershipControlsRequest.builder()
            .bucket(bucket)
            .ownershipControls(controls)
            .build());
}

public S3ObjectStreamWithResponse getObjectWithIfMatch(String bucketName, String objectKey, String etag) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .ifMatch(etag)
                .build();
        InputStream stream = s3.getObject(request);
        GetObjectResponse response = s3.getObject(request, software.amazon.awssdk.core.sync.ResponseTransformer.toBytes()).response();
        return new S3ObjectStreamWithResponse(stream, response);
    }

    // Helper class to hold both InputStream and GetObjectResponse
    public static class S3ObjectStreamWithResponse extends java.io.InputStream {
        private final InputStream stream;
        private final GetObjectResponse response;

        public S3ObjectStreamWithResponse(InputStream stream, GetObjectResponse response) {
            this.stream = stream;
            this.response = response;
        }

        public GetObjectResponse response() {
            return response;
        }

        @Override
        public int read() throws java.io.IOException {
            return stream.read();
        }

        @Override
        public int read(byte[] b) throws java.io.IOException {
            return stream.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws java.io.IOException {
            return stream.read(b, off, len);
        }

        @Override
        public void close() throws java.io.IOException {
            stream.close();
        }
    }

    public ResponseInputStream<GetObjectResponse> getObjectWithIfNoneMatch(String bucketName, String objectKey, String etag) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .ifNoneMatch(etag)
                .build();
        return s3.getObject(request);
    }

    public software.amazon.awssdk.services.s3.model.DeleteBucketResponse emptyBucketAllVersionsAndDelete(String bucket) {
        var client = s3;
        var paginator = client.listObjectVersionsPaginator(b -> b.bucket(bucket));
        var toDelete = new java.util.ArrayList<software.amazon.awssdk.services.s3.model.ObjectIdentifier>();
        for (var page : paginator) {
            if (page.versions() != null) {
                for (var v : page.versions()) {
                    toDelete.add(software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder().key(v.key()).versionId(v.versionId()).build());
                    if (toDelete.size() == 1000) {
                        client.deleteObjects(b -> b.bucket(bucket).delete(d -> d.objects(toDelete)));
                        toDelete.clear();
                    }
                }
            }
            if (page.deleteMarkers() != null) {
                for (var m : page.deleteMarkers()) {
                    toDelete.add(software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder().key(m.key()).versionId(m.versionId()).build());
                    if (toDelete.size() == 1000) {
                        client.deleteObjects(b -> b.bucket(bucket).delete(d -> d.objects(toDelete)));
                        toDelete.clear();
                    }
                }
            }
        }
        if (!toDelete.isEmpty()) {
            client.deleteObjects(b -> b.bucket(bucket).delete(d -> d.objects(toDelete)));
            toDelete.clear();
        }
        return client.deleteBucket(b -> b.bucket(bucket));
    }

}
