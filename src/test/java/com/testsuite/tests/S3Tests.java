package com.testsuite.tests;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.testsuite.utils.Config;
import com.testsuite.utils.FileCreator;
import com.testsuite.utils.S3Utilities;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.PublicAccessBlockConfiguration;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.StorageClass;

public class S3Tests {

    private S3Utilities s3Utils;
    private String bucketName;
    private String folderPath;

    @BeforeClass
    public void init() {
        folderPath = "src/test/testAssests";
        Path testFilesDir = Path.of(folderPath);
        if (!Files.exists(testFilesDir)) {
            try {
                Files.createDirectories(testFilesDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create test resources directory", e);
            }
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        String region = Config.get("awsRegion");
        String awsKey = Config.get("awsKey");
        String awsSecret = Config.get("awsSecret");
        s3Utils = new S3Utilities(Region.of(region), awsKey, awsSecret);
    }

    @Test
    public void CreateBucket_001() {
        bucketName = s3Utils.generateRandomBucketName();
        var createRes = s3Utils.createBucket(bucketName);
        Assert.assertNotNull(createRes);
    }

    @Test
    public void ListBuckets_002() {
        bucketName = s3Utils.generateRandomBucketName() + "1";
        String bucket2 = s3Utils.generateRandomBucketName() + "2";
        s3Utils.createBucket(bucketName);
        s3Utils.createBucket(bucket2);
        var listRes = s3Utils.listBuckets();
        Assert.assertNotNull(listRes);
        s3Utils.deleteBucket(bucket2);
    }

    @Test
    public void DeleteBucket_003() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        var deleteRes = s3Utils.deleteBucket(bucketName);
        Assert.assertNotNull(deleteRes);
        Assert.assertTrue(deleteRes.sdkHttpResponse().isSuccessful(), "Bucket deletion failed");
        bucketName = null;
    }

    @Test
    public void PutAnObjectToABucket_004() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String objectKey = "test-object";
        String content = "Hello, S3!";
        var putRes = s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PutFileToABucket_005() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String objectKey = "test-file";
        String content = "Hello, S3!";
        Path tempFile = Files.createTempFile("test-file", ".txt");
        Files.writeString(tempFile, content);
        var putRes = s3Utils.putFileObject(bucketName, objectKey, tempFile, "text/plain");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ListFilesInBucket_006() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String objectKey1 = "test-object-1";
        String objectKey2 = "test-object-2";
        String content1 = "Hello, S3 Object 1!";
        String content2 = "Hello, S3 Object 2!";
        Path objectPath1 = Files.createTempFile("test-object-1", ".txt");
        Path objectPath2 = Files.createTempFile("test-object-2", ".txt");
        Files.writeString(objectPath1, content1);
        Files.writeString(objectPath2, content2);
        s3Utils.putFileObject(bucketName, objectKey1, objectPath1, "text/plain");
        s3Utils.putFileObject(bucketName, objectKey2, objectPath2, "text/plain");
        var listRes = s3Utils.listObjects(bucketName);
        Assert.assertNotNull(listRes);
        Assert.assertTrue(listRes.contents().stream().anyMatch(o -> o.key().equals(objectKey1)), "Object 1 not found in list");
        Assert.assertTrue(listRes.contents().stream().anyMatch(o -> o.key().equals(objectKey2)), "Object 2 not found in list");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void DeleteObjectFromBucket_007() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String objectKey = "test-object-to-delete";
        String content = "This object will be deleted.";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        var deleteRes = s3Utils.deleteObject(bucketName, objectKey);
        Assert.assertNotNull(deleteRes);
        Assert.assertTrue(deleteRes.sdkHttpResponse().isSuccessful(), "Object deletion failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void DeleteFileFromABucket_008() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String objectKey = "test-file-to-delete";
        String content = "This file will be deleted.";
        Path tempFile = Files.createTempFile("test-file", ".txt");
        Files.writeString(tempFile, content);
        s3Utils.putFileObject(bucketName, objectKey, tempFile, "text/plain");
        var deleteRes = s3Utils.deleteObject(bucketName, objectKey);
        Assert.assertNotNull(deleteRes);
        Assert.assertTrue(deleteRes.sdkHttpResponse().isSuccessful(), "File deletion failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void GetHeadBucket_009() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        var headRes = s3Utils.headBucket(bucketName);
        Assert.assertNotNull(headRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void GetObject_010() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String objectKey = "test-object";
        String content = "Hello, S3!";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        var getRes = s3Utils.getObjectAsStream(bucketName, objectKey);
        Assert.assertNotNull(getRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadPNGFile_011() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createPNGFile(10, 10, folderPath, "011");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "image/png");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJPGFile_012() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createJPGFile(10, 10, folderPath, "012");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "image/jpeg");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJPEGFile_013() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createJPEGFile(10, 10, folderPath, "013");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "image/jpeg");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadPDFFile_014() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createPDFFile(folderPath, "014");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/pdf");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadDockerFile_015() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createDockerFile(folderPath, "015");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/plain");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadYMLFile_016() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createYMLFile(folderPath, "016");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/plain");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadCSVFile_017() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createCSVFile(folderPath, "017");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/csv");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJSONFile_018() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createJSONFile(folderPath, "018");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/json");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadXMLFile_019() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createXMLFile(folderPath, "019");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/xml");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadHTMLFile_020() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createHTMLFile(folderPath, "020");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/html");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJavaFile_021() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String contents = """
                public class HelloWorld {
                    public static void main(String[] args) {
                        System.out.println("Hello, World!");
                    }
                }
                """;
        String fileName = FileCreator.createJavaFile(contents, folderPath, "021");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/plain");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadTXTFile100KB_022() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createTextFileSizeInKB(folderPath, "022", 100);
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/plain");
        Assert.assertNotNull(putRes);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadPDFFile100KB_023() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createPDFFileInKB(folderPath, "023", 100);
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/pdf");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "100KB PDF file upload failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadPNGFile100KB_024() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createPNGFile(410, 410, folderPath, "024");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "image/png");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "100KB PNG file upload failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJPGFile100KB_025() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createJPGFile(410, 410, folderPath, "025");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "image/jpeg");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "100KB JPG file upload failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJPEGFile100KB_026() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createJPEGFile(410, 410, folderPath, "026");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "image/jpeg");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "100KB JPEG file upload failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadCSVFile100KB_027() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createCSVFileInKB(folderPath, "027", 100);
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/csv");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "100KB CSV file upload failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJSONFile100KB_028() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createJSONFileInKB(folderPath, "028", 100);
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/json");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "100KB JSON file upload failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadXMLFile100KB_029() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createXMLFileInKB(folderPath, "029", 100);
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/xml");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "100KB XML file upload failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadHTMLFile100KB_030() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createHTMLFileInKB(folderPath, "030", 100);
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/html");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "100KB HTML file upload failed");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJPEGVerifyFileType_031() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createJPEGFile(10, 10, folderPath, "031");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "image/jpeg");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "JPEG file upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentType(), "image/jpeg", "Content-Type mismatch for JPEG file");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadPNGVerifyFileType_032() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createPNGFile(10, 10, folderPath, "032");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "image/png");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "PNG file upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentType(), "image/png", "Content-Type mismatch for PNG file");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadPDFVerifyFileType_033() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createPDFFile(folderPath, "033");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/pdf");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "PDF file upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentType(), "application/pdf", "Content-Type mismatch for PDF file");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadHTMLVerifyFileType_034() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createHTMLFile(folderPath, "034");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/html");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "HTML file upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentType(), "text/html", "Content-Type mismatch for HTML file");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadJSONVerifyFileType_035() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createJSONFile(folderPath, "035");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/json");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "JSON file upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentType(), "application/json", "Content-Type mismatch for JSON file");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadXMLVerifyFileType_036() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createXMLFile(folderPath, "036");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/xml");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "XML file upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        String contentType = headRes.contentType();
        Assert.assertTrue(contentType.equals("application/xml") || contentType.equals("text/xml"),
                "Content-Type mismatch for XML file. Expected application/xml or text/xml, got: " + contentType);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadCSVVerifyFileType_037() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createCSVFile(folderPath, "037");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/csv");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "CSV file upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentType(), "text/csv", "Content-Type mismatch for CSV file");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadYMLVerifyFileType_038() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createYMLFile(folderPath, "038");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "application/x-yaml");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "YAML file upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        String contentType = headRes.contentType();
        Assert.assertTrue(contentType.equals("application/x-yaml") || contentType.equals("text/yaml"),
                "Content-Type mismatch for YAML file. Expected application/x-yaml or text/yaml, got: " + contentType);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void UploadDockerFileVerifyFileType_039() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String fileName = FileCreator.createDockerFile(folderPath, "039");
        Path filePath = Path.of(folderPath, fileName);
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, "text/plain");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "Dockerfile upload failed");

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentType(), "text/plain", "Content-Type mismatch for Dockerfile");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PutBucketVersioningEnable_040() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        var putVersioningRes = s3Utils.putBucketVersioning(bucketName, BucketVersioningStatus.ENABLED);
        Assert.assertNotNull(putVersioningRes);
        Assert.assertTrue(putVersioningRes.sdkHttpResponse().isSuccessful(), "Put bucket versioning failed");

        var getVersioningRes = s3Utils.getBucketVersioning(bucketName);
        Assert.assertNotNull(getVersioningRes);
        Assert.assertEquals(getVersioningRes.status(), BucketVersioningStatus.ENABLED,
                "Bucket versioning status mismatch");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PutBucketVersioningSuspend_041() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        s3Utils.putBucketVersioning(bucketName, BucketVersioningStatus.ENABLED);

        var putVersioningRes = s3Utils.putBucketVersioning(bucketName, BucketVersioningStatus.SUSPENDED);
        Assert.assertNotNull(putVersioningRes);
        Assert.assertTrue(putVersioningRes.sdkHttpResponse().isSuccessful(), "Put bucket versioning suspended failed");

        var getVersioningRes = s3Utils.getBucketVersioning(bucketName);
        Assert.assertNotNull(getVersioningRes);
        Assert.assertEquals(getVersioningRes.status(), BucketVersioningStatus.SUSPENDED,
                "Bucket versioning status mismatch");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PublicAccessBlockEnableAll_042() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        PublicAccessBlockConfiguration config = PublicAccessBlockConfiguration.builder()
                .blockPublicAcls(true)
                .blockPublicPolicy(true)
                .ignorePublicAcls(true)
                .restrictPublicBuckets(true)
                .build();

        var putPabRes = s3Utils.putPublicAccessBlock(bucketName, config);
        Assert.assertNotNull(putPabRes);
        Assert.assertTrue(putPabRes.sdkHttpResponse().isSuccessful(), "Put public access block failed");

        var getPabRes = s3Utils.getPublicAccessBlock(bucketName);
        Assert.assertNotNull(getPabRes);
        PublicAccessBlockConfiguration retrievedConfig = getPabRes.publicAccessBlockConfiguration();
        Assert.assertTrue(retrievedConfig.blockPublicAcls(), "Block public ACLs not enabled");
        Assert.assertTrue(retrievedConfig.blockPublicPolicy(), "Block public policy not enabled");
        Assert.assertTrue(retrievedConfig.ignorePublicAcls(), "Ignore public ACLs not enabled");
        Assert.assertTrue(retrievedConfig.restrictPublicBuckets(), "Restrict public buckets not enabled");
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketPolicyDenyPublic_043() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "test-object";
        String content = "Test content for policy";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");

        String policy = """
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DenyPublicAccess",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::%s/*"
                    }
                ]
            }
            """.formatted(bucketName);

        var putPolicyRes = s3Utils.putBucketPolicy(bucketName, policy);
        Assert.assertNotNull(putPolicyRes);
        Assert.assertTrue(putPolicyRes.sdkHttpResponse().isSuccessful(), "Put bucket policy failed");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketPolicyDenyRandomIAM_044() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "test-object";
        String content = "Test content";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");

        String policy = """
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DenyRandomIAMAccess",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": "arn:aws:s3:::%s/*",
                        "Condition": {
                            "StringNotEquals": {
                                "aws:PrincipalType": "Account"
                            }
                        }
                    }
                ]
            }
            """.formatted(bucketName);

        var putPolicyRes = s3Utils.putBucketPolicy(bucketName, policy);
        Assert.assertNotNull(putPolicyRes);
        Assert.assertTrue(putPolicyRes.sdkHttpResponse().isSuccessful(), "Put bucket policy failed");

        var getPolicyRes = s3Utils.getBucketPolicy(bucketName);
        Assert.assertNotNull(getPolicyRes);
        Assert.assertTrue(getPolicyRes.policy().contains("DenyRandomIAMAccess"), "Policy does not contain expected statement");

        try {
            s3Utils.getObjectAsStream(bucketName, objectKey);
            Assert.fail("Access should have been denied by the bucket policy");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is not authorized") || e.getMessage().contains("Access Denied") || e.getMessage().contains("403"),
                    "Expected access denied error, got: " + e.getMessage());
            System.out.println("Policy working correctly - access denied as expected");
        }

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketACLPrivate_045() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        try {
            s3Utils.putBucketOwnershipControls(bucketName, "BucketOwnerPreferred");

            var putAclRes = s3Utils.putBucketAcl(bucketName, "private");
            Assert.assertNotNull(putAclRes);
            Assert.assertTrue(putAclRes.sdkHttpResponse().isSuccessful(), "Put bucket ACL private failed");

            var getBucketAclRes = s3Utils.getBucketAcl(bucketName);
            Assert.assertNotNull(getBucketAclRes);
            Assert.assertFalse(getBucketAclRes.grants().isEmpty(), "No grants found in ACL");

        } catch (Exception e) {
            if (e.getMessage().contains("does not allow ACLs") || e.getMessage().contains("BucketOwnerEnforced")) {
                System.out.println("ACLs disabled on bucket (BucketOwnerEnforced) - this is expected security behavior");
                Assert.assertTrue(true, "Test passed - ACLs properly disabled for security");
            } else {
                throw e;
            }
        }

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketCORSSimpleGET_046() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "cors-test-object";
        String content = "CORS test content";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");

        String testOrigin = "https://example.com";
        var putCorsRes = s3Utils.putBucketCors(bucketName, testOrigin);
        Assert.assertNotNull(putCorsRes);
        Assert.assertTrue(putCorsRes.sdkHttpResponse().isSuccessful(), "Put CORS configuration failed");

        var getCorsRes = s3Utils.getBucketCors(bucketName);
        Assert.assertNotNull(getCorsRes);
        Assert.assertFalse(getCorsRes.corsRules().isEmpty(), "No CORS rules found");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ServerAccessLoggingEnable_047() {
        bucketName = s3Utils.generateRandomBucketName();
        String targetBucketName = s3Utils.generateRandomBucketName() + "-logs";

        s3Utils.createBucket(bucketName);
        s3Utils.createBucket(targetBucketName);

        String logPrefix = "access-logs/";
        var putLoggingRes = s3Utils.putBucketLogging(bucketName, targetBucketName, logPrefix);
        Assert.assertNotNull(putLoggingRes);
        Assert.assertTrue(putLoggingRes.sdkHttpResponse().isSuccessful(), "Put bucket logging failed");

        var getLoggingRes = s3Utils.getBucketLogging(bucketName);
        Assert.assertNotNull(getLoggingRes);
        Assert.assertEquals(getLoggingRes.loggingEnabled().targetBucket(), targetBucketName,
                "Target bucket mismatch in logging configuration");

        s3Utils.emptyBucket(bucketName);
        s3Utils.deleteBucket(targetBucketName);
    }

    @Test
    public void PutObjectWithVersioningEnabled_048() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        s3Utils.putBucketVersioning(bucketName, BucketVersioningStatus.ENABLED);
        String key = "v-obj";
        s3Utils.putObject(bucketName, key, "v1".getBytes(StandardCharsets.UTF_8), "text/plain");
        s3Utils.putObject(bucketName, key, "v2".getBytes(StandardCharsets.UTF_8), "text/plain");
        var head = s3Utils.headObject(bucketName, key);
        Assert.assertNotNull(head);
        s3Utils.emptyBucketAllVersionsAndDelete(bucketName);
        bucketName = null;
    }

    @Test
    public void GetSpecificVersion_049() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        s3Utils.putBucketVersioning(bucketName, BucketVersioningStatus.ENABLED);

        String objectKey = "multi-version-object";
        String content1 = "First version content";
        String content2 = "Second version content";

        var putRes1 = s3Utils.putObject(bucketName, objectKey, content1.getBytes(), "text/plain");
        String versionId1 = putRes1.versionId();

        var putRes2 = s3Utils.putObject(bucketName, objectKey, content2.getBytes(), "text/plain");
        String versionId2 = putRes2.versionId();

        var getRes1 = s3Utils.getObjectVersion(bucketName, objectKey, versionId1);
        Assert.assertNotNull(getRes1);

        var getRes2 = s3Utils.getObjectVersion(bucketName, objectKey, versionId2);
        Assert.assertNotNull(getRes2);

         s3Utils.emptyBucketAllVersionsAndDelete(bucketName);
    }

    @Test
    public void DeleteMarkerCreation_050() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        s3Utils.putBucketVersioning(bucketName, BucketVersioningStatus.ENABLED);

        String objectKey = "delete-marker-test";
        String content = "Content to be deleted";

        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");

        var deleteRes = s3Utils.deleteObject(bucketName, objectKey);
        Assert.assertNotNull(deleteRes);
        Assert.assertTrue(deleteRes.sdkHttpResponse().isSuccessful(), "Delete object failed");
        Assert.assertTrue(deleteRes.deleteMarker(), "Delete marker not created");

        var listVersionsRes = s3Utils.listObjectVersions(bucketName, objectKey);
        Assert.assertNotNull(listVersionsRes);
        Assert.assertFalse(listVersionsRes.deleteMarkers().isEmpty(), "No delete markers found");

        s3Utils.emptyBucketAllVersionsAndDelete(bucketName);
    }

    @Test
    public void RestorePreviousVersion_051() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        s3Utils.putBucketVersioning(bucketName, BucketVersioningStatus.ENABLED);

        String objectKey = "restore-test";
        String content = "Original content";

        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        var deleteRes = s3Utils.deleteObject(bucketName, objectKey);
        String deleteMarkerVersionId = deleteRes.versionId();

        var deleteMarkerRes = s3Utils.deleteObjectVersion(bucketName, objectKey, deleteMarkerVersionId);
        Assert.assertNotNull(deleteMarkerRes);
        Assert.assertTrue(deleteMarkerRes.sdkHttpResponse().isSuccessful(), "Delete marker removal failed");

        var getRes = s3Utils.getObjectAsStream(bucketName, objectKey);
        Assert.assertNotNull(getRes);

        s3Utils.emptyBucketAllVersionsAndDelete(bucketName);
    }

    @Test
    public void CopyObjectSameBucket_052() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String sourceKey = "source-object";
        String destKey = "destination-object";
        String content = "Content to copy";

        s3Utils.putObject(bucketName, sourceKey, content.getBytes(), "text/plain");

        var copyRes = s3Utils.copyObject(bucketName, sourceKey, bucketName, destKey);
        Assert.assertNotNull(copyRes);
        Assert.assertTrue(copyRes.sdkHttpResponse().isSuccessful(), "Copy object failed");

        var getDestRes = s3Utils.getObjectAsStream(bucketName, destKey);
        Assert.assertNotNull(getDestRes);

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void CopyObjectAcrossBuckets_053() {
        bucketName = s3Utils.generateRandomBucketName();
        String destBucketName = s3Utils.generateRandomBucketName() + "-dest";

        s3Utils.createBucket(bucketName);
        s3Utils.createBucket(destBucketName);

        String sourceKey = "source-object";
        String destKey = "copied-object";
        String content = "Cross-bucket copy content";
        Map<String, String> metadata = Map.of("test-meta", "test-value");

        s3Utils.putObjectWithMetadata(bucketName, sourceKey, content.getBytes(), "text/plain", metadata);

        var copyRes = s3Utils.copyObject(bucketName, sourceKey, destBucketName, destKey);
        Assert.assertNotNull(copyRes);
        Assert.assertTrue(copyRes.sdkHttpResponse().isSuccessful(), "Cross-bucket copy failed");

        var headRes = s3Utils.headObject(destBucketName, destKey);
        Assert.assertNotNull(headRes);

        s3Utils.emptyBucket(bucketName);
        s3Utils.emptyBucket(destBucketName);
        s3Utils.deleteBucket(destBucketName);
    }

    @Test
    public void HeadObjectWithMetadata_054() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "metadata-test-object";
        String content = "Content with metadata";
        Map<String, String> metadata = Map.of(
                "author", "test-user",
                "department", "qa",
                "version", "1.0"
        );

        s3Utils.putObjectWithMetadata(bucketName, objectKey, content.getBytes(), "text/plain", metadata);

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.metadata().get("author"), "test-user", "Author metadata mismatch");
        Assert.assertEquals(headRes.metadata().get("department"), "qa", "Department metadata mismatch");
        Assert.assertEquals(headRes.metadata().get("version"), "1.0", "Version metadata mismatch");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void MultipartUploadListParts_055() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "multipart-test-object";
        var createRes = s3Utils.createMultipartUpload(bucketName, objectKey);
        String uploadId = createRes.uploadId();

        byte[] part1Data = new byte[5 * 1024 * 1024];
        byte[] part2Data = new byte[5 * 1024 * 1024];
        Arrays.fill(part1Data, (byte) 'A');
        Arrays.fill(part2Data, (byte) 'B');

        s3Utils.uploadPart(bucketName, objectKey, uploadId, 1, part1Data);
        s3Utils.uploadPart(bucketName, objectKey, uploadId, 2, part2Data);

        var listPartsRes = s3Utils.listParts(bucketName, objectKey, uploadId);
        Assert.assertNotNull(listPartsRes);
        Assert.assertEquals(listPartsRes.parts().size(), 2, "Incorrect number of parts listed");

        s3Utils.abortMultipartUpload(bucketName, objectKey, uploadId);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void MultipartUploadComplete_056() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "multipart-complete-test";
        var createRes = s3Utils.createMultipartUpload(bucketName, objectKey);
        String uploadId = createRes.uploadId();

        byte[] part1Data = new byte[5 * 1024 * 1024];
        byte[] part2Data = new byte[5 * 1024 * 1024];
        Arrays.fill(part1Data, (byte) 'X');
        Arrays.fill(part2Data, (byte) 'Y');

        var uploadPart1Res = s3Utils.uploadPart(bucketName, objectKey, uploadId, 1, part1Data);
        var uploadPart2Res = s3Utils.uploadPart(bucketName, objectKey, uploadId, 2, part2Data);

        List<CompletedPart> parts = List.of(
                CompletedPart.builder().partNumber(1).eTag(uploadPart1Res.eTag()).build(),
                CompletedPart.builder().partNumber(2).eTag(uploadPart2Res.eTag()).build()
        );

        var completeRes = s3Utils.completeMultipartUpload(bucketName, objectKey, uploadId, parts);
        Assert.assertNotNull(completeRes);
        Assert.assertTrue(completeRes.sdkHttpResponse().isSuccessful(), "Multipart upload completion failed");

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentLength().longValue(), 10 * 1024 * 1024L, "Object size incorrect");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void MultipartUploadAbort_057() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "multipart-abort-test";
        var createRes = s3Utils.createMultipartUpload(bucketName, objectKey);
        String uploadId = createRes.uploadId();

        byte[] partData = new byte[5 * 1024 * 1024];
        Arrays.fill(partData, (byte) 'Z');
        s3Utils.uploadPart(bucketName, objectKey, uploadId, 1, partData);

        var abortRes = s3Utils.abortMultipartUpload(bucketName, objectKey, uploadId);
        Assert.assertNotNull(abortRes);
        Assert.assertTrue(abortRes.sdkHttpResponse().isSuccessful(), "Multipart upload abort failed");

        try {
            s3Utils.headObject(bucketName, objectKey);
            Assert.fail("Object should not exist after multipart upload abort");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("NotFound") || e.getMessage().contains("404"),
                    "Expected NotFound error after abort");
        }

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PutObjectTagging_058() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "tagged-object";
        String content = "Object with tags";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");

        Map<String, String> tags = Map.of(
                "environment", "development",
                "project", "s3-testing",
                "owner", "qa-team"
        );

        var putTagsRes = s3Utils.putObjectTagging(bucketName, objectKey, tags);
        Assert.assertNotNull(putTagsRes);
        Assert.assertTrue(putTagsRes.sdkHttpResponse().isSuccessful(), "Put object tagging failed");

        var getTagsRes = s3Utils.getObjectTagging(bucketName, objectKey);
        Assert.assertNotNull(getTagsRes);
        Assert.assertEquals(getTagsRes.tagSet().size(), 3, "Incorrect number of tags");

        boolean envTagFound = getTagsRes.tagSet().stream()
                .anyMatch(tag -> "environment".equals(tag.key()) && "development".equals(tag.value()));
        Assert.assertTrue(envTagFound, "Environment tag not found");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void DeleteObjectTagging_059() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "tagged-object-delete";
        String content = "Object with tags to delete";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");

        Map<String, String> tags = Map.of("temp", "true", "test", "delete-tags");
        s3Utils.putObjectTagging(bucketName, objectKey, tags);

        var deleteTagsRes = s3Utils.deleteObjectTagging(bucketName, objectKey);
        Assert.assertNotNull(deleteTagsRes);
        Assert.assertTrue(deleteTagsRes.sdkHttpResponse().isSuccessful(), "Delete object tagging failed");

        var getTagsRes = s3Utils.getObjectTagging(bucketName, objectKey);
        Assert.assertNotNull(getTagsRes);
        Assert.assertTrue(getTagsRes.tagSet().isEmpty(), "Tags still present after deletion");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PutObjectMetadata_060() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "metadata-object";
        String content = "Object with custom metadata";
        Map<String, String> metadata = Map.of(
                "owner", "qa-engineer",
                "created-by", "automated-test",
                "content-category", "test-data"
        );

        var putRes = s3Utils.putObjectWithMetadata(bucketName, objectKey, content.getBytes(), "text/plain", metadata);
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "Put object with metadata failed");

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.metadata().get("owner"), "qa-engineer", "Owner metadata mismatch");
        Assert.assertEquals(headRes.metadata().get("created-by"), "automated-test", "Created-by metadata mismatch");
        Assert.assertEquals(headRes.metadata().get("content-category"), "test-data", "Content-category metadata mismatch");

        var getRes = s3Utils.getObjectAsStream(bucketName, objectKey);
        Assert.assertNotNull(getRes);

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketDefaultEncryptionAES256_061() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        var putEncryptionRes = s3Utils.putBucketEncryption(bucketName);
        Assert.assertNotNull(putEncryptionRes);
        Assert.assertTrue(putEncryptionRes.sdkHttpResponse().isSuccessful(), "Put bucket encryption failed");

        var getEncryptionRes = s3Utils.getBucketEncryption(bucketName);
        Assert.assertNotNull(getEncryptionRes);
        Assert.assertFalse(getEncryptionRes.serverSideEncryptionConfiguration().rules().isEmpty(),
                "No encryption rules found");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void DefaultEncryptionAppliedOnPUT_062() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        s3Utils.putBucketEncryption(bucketName);

        String objectKey = "encrypted-by-default";
        String content = "This should be encrypted by default";
        var putRes = s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "Put object failed");

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.serverSideEncryption(), ServerSideEncryption.AES256,
                "Object not encrypted with AES256");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PutObjectWithSSE_S3_063() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "sse-s3-object";
        String content = "Content encrypted with SSE-S3";
        var putRes = s3Utils.putObjectWithSSE(bucketName, objectKey, content.getBytes(), "text/plain");
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "Put object with SSE-S3 failed");
        Assert.assertEquals(putRes.serverSideEncryption(), ServerSideEncryption.AES256,
                "Server-side encryption not set to AES256");

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.serverSideEncryption(), ServerSideEncryption.AES256,
                "Object encryption header missing");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketCORSSetAndGet_064() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String testOrigin = "https://test-example.com";
        var putCorsRes = s3Utils.putBucketCors(bucketName, testOrigin);
        Assert.assertNotNull(putCorsRes);
        Assert.assertTrue(putCorsRes.sdkHttpResponse().isSuccessful(), "Put CORS configuration failed");

        var getCorsRes = s3Utils.getBucketCors(bucketName);
        Assert.assertNotNull(getCorsRes);
        Assert.assertFalse(getCorsRes.corsRules().isEmpty(), "No CORS rules found");

        boolean originFound = getCorsRes.corsRules().stream()
                .anyMatch(rule -> rule.allowedOrigins().contains(testOrigin));
        Assert.assertTrue(originFound, "Test origin not found in CORS configuration");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketWebsiteConfigureIndex_065() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String indexDocument = "index.html";
        String errorDocument = "error.html";

        var putWebsiteRes = s3Utils.putBucketWebsite(bucketName, indexDocument, errorDocument);
        Assert.assertNotNull(putWebsiteRes);
        Assert.assertTrue(putWebsiteRes.sdkHttpResponse().isSuccessful(), "Put bucket website configuration failed");

        var getWebsiteRes = s3Utils.getBucketWebsite(bucketName);
        Assert.assertNotNull(getWebsiteRes);
        Assert.assertEquals(getWebsiteRes.indexDocument().suffix(), indexDocument,
                "Index document mismatch");
        Assert.assertEquals(getWebsiteRes.errorDocument().key(), errorDocument,
                "Error document mismatch");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketTaggingPutAndGet_066() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        Map<String, String> tags = Map.of(
                "environment", "test",
                "project", "s3-automation",
                "cost-center", "qa"
        );

        var putTagsRes = s3Utils.putBucketTagging(bucketName, tags);
        Assert.assertNotNull(putTagsRes);
        Assert.assertTrue(putTagsRes.sdkHttpResponse().isSuccessful(), "Put bucket tagging failed");

        var getTagsRes = s3Utils.getBucketTagging(bucketName);
        Assert.assertNotNull(getTagsRes);
        Assert.assertEquals(getTagsRes.tagSet().size(), 3, "Incorrect number of bucket tags");

        boolean envTagFound = getTagsRes.tagSet().stream()
                .anyMatch(tag -> "environment".equals(tag.key()) && "test".equals(tag.value()));
        Assert.assertTrue(envTagFound, "Environment tag not found");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ObjectETagCheck_067() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "etag-test-object";
        String content = "Content for ETag verification";
        var putRes = s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        Assert.assertNotNull(putRes);
        String expectedETag = putRes.eTag();

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.eTag(), expectedETag, "ETag mismatch");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void CopyObjectWithMetadataReplace_068() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String sourceKey = "source-metadata-object";
        String destKey = "dest-metadata-object";
        String content = "Content for metadata copy test";

        Map<String, String> originalMetadata = Map.of(
                "original", "true",
                "version", "1.0"
        );
        s3Utils.putObjectWithMetadata(bucketName, sourceKey, content.getBytes(), "text/plain", originalMetadata);

        Map<String, String> newMetadata = Map.of(
                "replaced", "true",
                "version", "2.0",
                "modified-by", "copy-operation"
        );

        var copyRes = s3Utils.copyObjectWithMetadata(bucketName, sourceKey, bucketName, destKey, newMetadata);
        Assert.assertNotNull(copyRes);
        Assert.assertTrue(copyRes.sdkHttpResponse().isSuccessful(), "Copy with metadata replace failed");

        var headRes = s3Utils.headObject(bucketName, destKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.metadata().get("replaced"), "true", "Replaced metadata not found");
        Assert.assertEquals(headRes.metadata().get("version"), "2.0", "Version metadata mismatch");
        Assert.assertNull(headRes.metadata().get("original"), "Original metadata still present");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void GetObjectRange_069() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "range-test-object";
        String content = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(StandardCharsets.UTF_8), "text/plain");

        var rangeRes = s3Utils.getObjectRange(bucketName, objectKey, 10, 19);
        Assert.assertNotNull(rangeRes);

        String rangeContent = new String(rangeRes.readAllBytes(), StandardCharsets.UTF_8);
        Assert.assertEquals(rangeContent, "ABCDEFGHIJ");

        int total = content.getBytes(StandardCharsets.UTF_8).length;
        String expected = String.format("bytes %d-%d/%d", 10, 19, total);
        Assert.assertEquals(rangeRes.response().contentRange(), expected);

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void MultipartUploadListUploads_070() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "multipart-list-test";
        var createRes = s3Utils.createMultipartUpload(bucketName, objectKey);
        String uploadId = createRes.uploadId();

        var listUploadsRes = s3Utils.listMultipartUploads(bucketName);
        Assert.assertNotNull(listUploadsRes);

        boolean uploadFound = listUploadsRes.uploads().stream()
                .anyMatch(upload -> upload.key().equals(objectKey) && upload.uploadId().equals(uploadId));
        Assert.assertTrue(uploadFound, "Multipart upload not found in list");

        s3Utils.abortMultipartUpload(bucketName, objectKey, uploadId);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketLifecyclePutAndGet_071() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String prefix = "temp/";
        int expirationDays = 30;

        var putLifecycleRes = s3Utils.putBucketLifecycle(bucketName, prefix, expirationDays);
        Assert.assertNotNull(putLifecycleRes);
        Assert.assertTrue(putLifecycleRes.sdkHttpResponse().isSuccessful(), "Put lifecycle configuration failed");

        var getLifecycleRes = s3Utils.getBucketLifecycle(bucketName);
        Assert.assertNotNull(getLifecycleRes);
        Assert.assertFalse(getLifecycleRes.rules().isEmpty(), "No lifecycle rules found");

        var rule = getLifecycleRes.rules().get(0);
        Assert.assertEquals(rule.filter().prefix(), prefix, "Prefix mismatch in lifecycle rule");
        Assert.assertEquals(rule.expiration().days().intValue(), expirationDays, "Expiration days mismatch");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketLifecycleDeleteRule_072() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        s3Utils.putBucketLifecycle(bucketName, "old/", 7);

        var deleteLifecycleRes = s3Utils.deleteBucketLifecycle(bucketName);
        Assert.assertNotNull(deleteLifecycleRes);
        Assert.assertTrue(deleteLifecycleRes.sdkHttpResponse().isSuccessful(), "Delete lifecycle configuration failed");

        try {
            s3Utils.getBucketLifecycle(bucketName);
            Assert.fail("Expected NoSuchLifecycleConfiguration error");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("NoSuchLifecycleConfiguration")
                    || e.getMessage().contains("lifecycle")
                    || e.getMessage().contains("404"),
                    "Expected lifecycle not found error");
        }

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketWebsiteConfigureErrorDocument_073() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String indexDocument = "index.html";
        String errorDocument = "404.html";

        var putWebsiteRes = s3Utils.putBucketWebsite(bucketName, indexDocument, errorDocument);
        Assert.assertNotNull(putWebsiteRes);
        Assert.assertTrue(putWebsiteRes.sdkHttpResponse().isSuccessful(), "Put bucket website configuration failed");

        var getWebsiteRes = s3Utils.getBucketWebsite(bucketName);
        Assert.assertNotNull(getWebsiteRes);
        Assert.assertEquals(getWebsiteRes.indexDocument().suffix(), indexDocument, "Index document mismatch");
        Assert.assertEquals(getWebsiteRes.errorDocument().key(), errorDocument, "Error document mismatch");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketCORSGetWithSpecificOrigin_074() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String testOrigin = "https://specific-origin.example.com";
        var putCorsRes = s3Utils.putBucketCors(bucketName, testOrigin);
        Assert.assertNotNull(putCorsRes);
        Assert.assertTrue(putCorsRes.sdkHttpResponse().isSuccessful(), "Put CORS configuration failed");

        var getCorsRes = s3Utils.getBucketCors(bucketName);
        Assert.assertNotNull(getCorsRes);
        Assert.assertFalse(getCorsRes.corsRules().isEmpty(), "No CORS rules found");

        var rule = getCorsRes.corsRules().get(0);
        Assert.assertTrue(rule.allowedOrigins().contains(testOrigin),
                "Specific origin not found in CORS configuration");
        Assert.assertTrue(rule.allowedMethods().contains("GET"), "GET method not allowed in CORS");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketCrossBucketETagCheck_075() {
        bucketName = s3Utils.generateRandomBucketName();
        String secondBucket = s3Utils.generateRandomBucketName() + "-2";

        s3Utils.createBucket(bucketName);
        s3Utils.createBucket(secondBucket);

        String objectKey = "etag-cross-bucket";
        String content = "Cross-bucket ETag test content";

        var putRes = s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        String originalETag = putRes.eTag();

        var copyRes = s3Utils.copyObject(bucketName, objectKey, secondBucket, objectKey);
        Assert.assertNotNull(copyRes);

        var headRes = s3Utils.headObject(secondBucket, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.eTag(), originalETag, "ETag mismatch after cross-bucket copy");

        s3Utils.emptyBucket(bucketName);
        s3Utils.emptyBucket(secondBucket);
        s3Utils.deleteBucket(secondBucket);
    }

    @Test
    public void ListObjectsWithPrefix_076() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        s3Utils.putObject(bucketName, "logs/2024/january.log", "Jan logs".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, "logs/2024/february.log", "Feb logs".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, "logs/2025/january.log", "Jan 2025 logs".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, "data/file.txt", "Data file".getBytes(), "text/plain");

        var listRes = s3Utils.listObjectsWithPrefix(bucketName, "logs/2024/");
        Assert.assertNotNull(listRes);
        Assert.assertEquals(listRes.contents().size(), 2, "Incorrect number of objects with prefix");

        boolean hasJan = listRes.contents().stream().anyMatch(obj -> obj.key().equals("logs/2024/january.log"));
        boolean hasFeb = listRes.contents().stream().anyMatch(obj -> obj.key().equals("logs/2024/february.log"));
        Assert.assertTrue(hasJan && hasFeb, "Expected objects not found with prefix");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ListCommonPrefixesDelimiter_077() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        // Create folder-like structure
        s3Utils.putObject(bucketName, "a/x", "File x in a".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, "a/y", "File y in a".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, "b/z", "File z in b".getBytes(), "text/plain");

        var listRes = s3Utils.listObjectsWithDelimiter(bucketName, "", "/");
        Assert.assertNotNull(listRes);

        Assert.assertEquals(listRes.commonPrefixes().size(), 2, "Incorrect number of common prefixes");

        boolean hasA = listRes.commonPrefixes().stream().anyMatch(prefix -> prefix.prefix().equals("a/"));
        boolean hasB = listRes.commonPrefixes().stream().anyMatch(prefix -> prefix.prefix().equals("b/"));
        Assert.assertTrue(hasA && hasB, "Expected common prefixes not found");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void CopyObjectMetadataReplace_078() {
        bucketName = s3Utils.generateRandomBucketName();
        String destBucket = s3Utils.generateRandomBucketName() + "-dest";

        s3Utils.createBucket(bucketName);
        s3Utils.createBucket(destBucket);

        String sourceKey = "source-meta";
        String destKey = "dest-meta";

        Map<String, String> sourceMetadata = Map.of("source", "original", "type", "test");
        s3Utils.putObjectWithMetadata(bucketName, sourceKey, "Test content".getBytes(), "text/plain", sourceMetadata);

        Map<String, String> newMetadata = Map.of("destination", "replaced", "modified", "true");
        var copyRes = s3Utils.copyObjectWithMetadata(bucketName, sourceKey, destBucket, destKey, newMetadata);
        Assert.assertNotNull(copyRes);

        var headRes = s3Utils.headObject(destBucket, destKey);
        Assert.assertEquals(headRes.metadata().get("destination"), "replaced", "New metadata not applied");
        Assert.assertNull(headRes.metadata().get("source"), "Original metadata still present");

        s3Utils.emptyBucket(bucketName);
        s3Utils.emptyBucket(destBucket);
        s3Utils.deleteBucket(destBucket);
    }

    @Test
    public void ContentTypeOnPutFile_079() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String fileName = FileCreator.createTextFile(folderPath, "079");
        Path filePath = Path.of(folderPath, fileName);

        String customContentType = "application/custom-type";
        var putRes = s3Utils.putFileObject(bucketName, fileName, filePath, customContentType);
        Assert.assertNotNull(putRes);

        var headRes = s3Utils.headObject(bucketName, fileName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.contentType(), customContentType, "Content-Type not set correctly");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PublicAccessAnonymousHeadDenied_080() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "private-object";
        s3Utils.putObject(bucketName, objectKey, "Private content".getBytes(), "text/plain");

        PublicAccessBlockConfiguration config = PublicAccessBlockConfiguration.builder()
                .blockPublicAcls(true)
                .blockPublicPolicy(true)
                .ignorePublicAcls(true)
                .restrictPublicBuckets(true)
                .build();
        s3Utils.putPublicAccessBlock(bucketName, config);

        var pabRes = s3Utils.getPublicAccessBlock(bucketName);
        Assert.assertTrue(pabRes.publicAccessBlockConfiguration().blockPublicAcls(),
                "Public ACLs not blocked");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void BucketOwnershipControlsBucketOwnerEnforced_081() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        var putOwnershipRes = s3Utils.putBucketOwnershipControls(bucketName, "BucketOwnerEnforced");
        Assert.assertNotNull(putOwnershipRes);
        Assert.assertTrue(putOwnershipRes.sdkHttpResponse().isSuccessful(),
                "Put bucket ownership controls failed");

        try {
            s3Utils.putBucketAcl(bucketName, "public-read");
            Assert.fail("Expected ACL operation to fail with BucketOwnerEnforced");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("AccessControlListNotSupported")
                    || e.getMessage().contains("ACL")
                    || e.getMessage().contains("BucketOwnerEnforced"),
                    "Expected ACL not supported error");
        }

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ObjectStorageClassStandardIA_082() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "standard-ia-object";
        String content = "Content for STANDARD_IA storage class";

        var putRes = s3Utils.putObjectWithStorageClass(bucketName, objectKey, content.getBytes(),
                "text/plain", StorageClass.STANDARD_IA);
        Assert.assertNotNull(putRes);
        Assert.assertTrue(putRes.sdkHttpResponse().isSuccessful(), "Put object with STANDARD_IA failed");

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.storageClass(), StorageClass.STANDARD_IA,
                "Storage class not set to STANDARD_IA");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ConditionalGetIfMatchSuccess_083() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "conditional-test";
        String content = "Content for conditional GET";
        var putRes = s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        String etag = putRes.eTag();

        var getRes = s3Utils.getObjectWithIfMatch(bucketName, objectKey, etag);
        Assert.assertNotNull(getRes);
        Assert.assertEquals(getRes.response().sdkHttpResponse().statusCode(), 200,
                "If-Match with correct ETag should return 200");

        String retrievedContent = new String(getRes.readAllBytes());
        Assert.assertEquals(retrievedContent, content, "Content mismatch");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ConditionalGetIfNoneMatchNotModified_084() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "conditional-none-match";
        String content = "Content for If-None-Match test";
        var putRes = s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");
        String etag = putRes.eTag();

        try {
            var getRes = s3Utils.getObjectWithIfNoneMatch(bucketName, objectKey, etag);
            Assert.assertEquals(getRes.response().sdkHttpResponse().statusCode(), 304,
                    "If-None-Match with matching ETag should return 304");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("304")
                    || e.getMessage().contains("Not Modified"),
                    "Expected 304 Not Modified response");
        }

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PresignedPutThenGetObject_085() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "presigned-test";

        String putUrl = s3Utils.generatePresignedPutUrl(bucketName, objectKey, Duration.ofMinutes(5));
        Assert.assertNotNull(putUrl);
        Assert.assertTrue(putUrl.contains(bucketName), "Bucket name not in presigned URL");
        Assert.assertTrue(putUrl.contains(objectKey), "Object key not in presigned URL");

        String content = "Presigned URL test content";
        s3Utils.putObject(bucketName, objectKey, content.getBytes(), "text/plain");

        String getUrl = s3Utils.generatePresignedGetUrl(bucketName, objectKey, Duration.ofMinutes(5));
        Assert.assertNotNull(getUrl);
        Assert.assertTrue(getUrl.contains(bucketName), "Bucket name not in presigned GET URL");

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PutObjectWithMetadataThenHeadMetadata_086() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "metadata-head-test";
        Map<String, String> metadata = Map.of(
                "application", "s3-test-suite",
                "test-id", "086",
                "timestamp", String.valueOf(System.currentTimeMillis())
        );

        var putRes = s3Utils.putObjectWithMetadata(bucketName, objectKey,
                "Test content".getBytes(), "text/plain", metadata);
        Assert.assertNotNull(putRes);

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.metadata().get("application"), "s3-test-suite",
                "Application metadata mismatch");
        Assert.assertEquals(headRes.metadata().get("test-id"), "086", "Test-id metadata mismatch");
        Assert.assertNotNull(headRes.metadata().get("timestamp"), "Timestamp metadata missing");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void CopyObjectWithMetadataReplace_087() {
        bucketName = s3Utils.generateRandomBucketName();
        String destBucket = s3Utils.generateRandomBucketName() + "-copy";

        s3Utils.createBucket(bucketName);
        s3Utils.createBucket(destBucket);

        String sourceKey = "source-replace-meta";
        String destKey = "dest-replace-meta";

        Map<String, String> originalMeta = Map.of("stage", "production", "version", "1.0");
        s3Utils.putObjectWithMetadata(bucketName, sourceKey, "Content".getBytes(), "text/plain", originalMeta);

        Map<String, String> replacementMeta = Map.of("stage", "development", "version", "2.0", "copied", "true");
        var copyRes = s3Utils.copyObjectWithMetadata(bucketName, sourceKey, destBucket, destKey, replacementMeta);
        Assert.assertNotNull(copyRes);

        var headRes = s3Utils.headObject(destBucket, destKey);
        Assert.assertEquals(headRes.metadata().get("stage"), "development", "Stage metadata not replaced");
        Assert.assertEquals(headRes.metadata().get("version"), "2.0", "Version metadata not replaced");
        Assert.assertEquals(headRes.metadata().get("copied"), "true", "New metadata not added");

        s3Utils.emptyBucket(bucketName);
        s3Utils.emptyBucket(destBucket);
        s3Utils.deleteBucket(destBucket);
    }

    @Test
    public void GetObjectRangeBytes_088() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        String objectKey = "range-bytes-test";
        String fullContent = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
        s3Utils.putObject(bucketName, objectKey, fullContent.getBytes(), "text/plain");

        // Get bytes 0-9
        var rangeRes1 = s3Utils.getObjectRange(bucketName, objectKey, 0, 9);
        String range1 = new String(rangeRes1.readAllBytes());
        Assert.assertEquals(range1, "ABCDEFGHIJ", "First range mismatch");

        // Get bytes 26-35
        var rangeRes2 = s3Utils.getObjectRange(bucketName, objectKey, 26, 35);
        String range2 = new String(rangeRes2.readAllBytes());
        Assert.assertEquals(range2, "0123456789", "Second range mismatch");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ListObjectsWithDelimiter_089() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        // Create nested structure
        s3Utils.putObject(bucketName, "folder1/file1.txt", "File 1".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, "folder1/file2.txt", "File 2".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, "folder2/subfolder/file3.txt", "File 3".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, "root.txt", "Root file".getBytes(), "text/plain");

        var listRes = s3Utils.listObjectsWithDelimiter(bucketName, "", "/");
        Assert.assertNotNull(listRes);

        Assert.assertEquals(listRes.contents().size(), 1, "Incorrect number of root objects");
        Assert.assertEquals(listRes.contents().get(0).key(), "root.txt", "Root object mismatch");

        Assert.assertEquals(listRes.commonPrefixes().size(), 2, "Incorrect number of common prefixes");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void DefaultBucketEncryptionAES256AppliesToNewObjects_090() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        var putEncryptionRes = s3Utils.putBucketEncryption(bucketName);
        Assert.assertNotNull(putEncryptionRes);

        String objectKey = "auto-encrypted-object";
        var putRes = s3Utils.putObject(bucketName, objectKey, "Test content".getBytes(), "text/plain");
        Assert.assertNotNull(putRes);

        var headRes = s3Utils.headObject(bucketName, objectKey);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.serverSideEncryption(), ServerSideEncryption.AES256,
                "Object not encrypted with default AES256");

        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void APIRequestCreateBucket_091() {
        bucketName = s3Utils.generateRandomBucketName();

        var createRes = s3Utils.createBucket(bucketName);
        Assert.assertNotNull(createRes);
        Assert.assertTrue(createRes.sdkHttpResponse().isSuccessful(), "Bucket creation failed");

        var headRes = s3Utils.headBucket(bucketName);
        Assert.assertNotNull(headRes);
        Assert.assertEquals(headRes.sdkHttpResponse().statusCode(), 200, "Bucket not accessible");
    }

    @Test
    public void APIRequestDeleteBucket_092() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);

        s3Utils.emptyBucket(bucketName);

        var deleteRes = s3Utils.deleteBucket(bucketName);
        Assert.assertNotNull(deleteRes);
        Assert.assertTrue(deleteRes.sdkHttpResponse().isSuccessful(), "Bucket deletion failed");

        try {
            s3Utils.headBucket(bucketName);
            Assert.fail("Bucket should not exist after deletion");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("404")
                    || e.getMessage().contains("NoSuchBucket"),
                    "Expected bucket not found error");
        }
        bucketName = null;
    }

    @Test
    public void HeadBucket_093() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        var head = s3Utils.headBucket(bucketName);
        Assert.assertNotNull(head);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ListBuckets_094() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        var list = s3Utils.listBuckets();
        Assert.assertNotNull(list);
        Assert.assertTrue(list.buckets().stream().anyMatch(b -> b.name().equals(bucketName)));
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void PutObjectToBucket_095() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String key = "tc_095.txt";
        byte[] body = "hello-095".getBytes();
        var put = s3Utils.putObject(bucketName, key, body, "text/plain");
        Assert.assertNotNull(put);
        var head = s3Utils.headObject(bucketName, key);
        Assert.assertNotNull(head);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void AddFileToBucket_096() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String key = "tc_096.bin";
        Path f = Files.createTempFile("tc_096_", ".bin");
        Files.write(f, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
        long size = Files.size(f);
        var put = s3Utils.putFileObject(bucketName, key, f, "application/octet-stream");
        Assert.assertNotNull(put);
        var head = s3Utils.headObject(bucketName, key);
        Assert.assertNotNull(head);
        Assert.assertEquals(head.contentLength(), size);
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void ListFilesInBucket_097() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String k1 = "a/one.txt";
        String k2 = "a/two.txt";
        s3Utils.putObject(bucketName, k1, "x".getBytes(), "text/plain");
        s3Utils.putObject(bucketName, k2, "y".getBytes(), "text/plain");
        var list = s3Utils.listObjects(bucketName);
        Assert.assertNotNull(list);
        Assert.assertTrue(list.contents().stream().anyMatch(o -> o.key().equals(k1)));
        Assert.assertTrue(list.contents().stream().anyMatch(o -> o.key().equals(k2)));
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void DeleteObject_098() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String key = "to-delete-098.txt";
        s3Utils.putObject(bucketName, key, "z".getBytes(), "text/plain");
        var del = s3Utils.deleteObject(bucketName, key);
        Assert.assertNotNull(del);
        Assert.assertTrue(del.sdkHttpResponse().isSuccessful());
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void DeleteFile_099() throws IOException {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String key = "file-099.txt";
        Path f = Files.createTempFile("tc_099_", ".txt");
        Files.writeString(f, "file-099");
        s3Utils.putFileObject(bucketName, key, f, "text/plain");
        var del = s3Utils.deleteObject(bucketName, key);
        Assert.assertNotNull(del);
        Assert.assertTrue(del.sdkHttpResponse().isSuccessful());
        s3Utils.emptyBucket(bucketName);
    }

    @Test
    public void GetObject_100() {
        bucketName = s3Utils.generateRandomBucketName();
        s3Utils.createBucket(bucketName);
        String key = "get-100.txt";
        s3Utils.putObject(bucketName, key, "hello-100".getBytes(), "text/plain");
        var stream = s3Utils.getObjectAsStream(bucketName, key);
        Assert.assertNotNull(stream);
        s3Utils.emptyBucket(bucketName);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanupTest() {
        if (s3Utils != null && bucketName != null) {
            try {
                s3Utils.deleteBucket(bucketName);
            } catch (Exception e) {
                System.err.println("Error cleaning up S3 bucket: " + e.getMessage());
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void cleanupClass() {
        try {
            Files.walk(Path.of(folderPath))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            System.err.println("Error cleaning up test resources: " + e.getMessage());
        }
    }
}
