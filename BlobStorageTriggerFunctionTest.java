package com.statestr.eff.spark.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.microsoft.azure.functions.ExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.functions.ExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.microsoft.azure.functions.ExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.azure.functions.ExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.URI;

import java.util.Date;

public class BlobStorageTriggerFunctionTest {

    @Mock
    ExecutionContext mockContext;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testBlobTriggerBehavior() {
        // Mocking blob and filepath
        String blob = "Sample Blob Data";
        String filepath = "test_blob.txt";

        // Mocking AzureWebJobsStorage and containerName environment variables
        System.setProperty("AZURE_STORAGE_CONTAINER", "test_container");
        System.setProperty("AzureWebJobsStorage", "test_connection_string");

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());

        // Mocking CloudBlobClient and CloudBlockBlob
        CloudBlobClient mockBlobClient = mock(CloudBlobClient.class);
        CloudBlockBlob mockBlockBlob = mock(CloudBlockBlob.class);
        when(function.getCloudBlobClient()).thenReturn(mockBlobClient);
        when(mockBlobClient.getContainerReference(anyString())).thenReturn(mock(CloudBlobContainer.class));
        when(mockBlockBlob.getProperties()).thenReturn(mock(BlobProperties.class));
        when(mockBlobClient.getContainerReference(anyString()).getBlockBlobReference(anyString())).thenReturn(mockBlockBlob);

        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Invoking the run method of BlobStorageTriggerFunction
        function.run(blob, filepath, mockContext);

        // Verify that the necessary methods are called
        verify(mockBlockBlob, times(1)).downloadAttributes();
        verify(mockBlobClient, times(1)).getContainerReference(anyString());
        verify(mockBlockBlob, times(1)).downloadAttributes();
    }

    @Test
    public void testEmptyBlobName() {
        // Mocking blob and filepath with empty blobname
        String blob = "Sample Blob Data";
        String filepath = "";

        // Mocking ExecutionContext
        ExecutionContext mockContext = mock(ExecutionContext.class);
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());

        // Invoking the run method of BlobStorageTriggerFunction with empty blob name
        function.run(blob, filepath, mockContext);

        // Verify that logger logs severe message for empty payload
        verify(mockContext.getLogger(), times(1)).severe("Empty Payload");
    }

    @Test
    public void testDownloadBlobAttributes() throws Exception {
        // Mocking blob and filepath
        String blob = "Sample Blob Data";
        String filepath = "test_blob.txt";

        // Mocking AzureWebJobsStorage and containerName environment variables
        System.setProperty("AZURE_STORAGE_CONTAINER", "test_container");
        System.setProperty("AzureWebJobsStorage", "test_connection_string");

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());

        // Mocking CloudBlobClient and CloudBlockBlob
        CloudBlobClient mockBlobClient = mock(CloudBlobClient.class);
        CloudBlockBlob mockBlockBlob = mock(CloudBlockBlob.class);
        BlobProperties mockBlobProperties = mock(BlobProperties.class);

        when(function.getCloudBlobClient()).thenReturn(mockBlobClient);
        when(mockBlobClient.getContainerReference(anyString())).thenReturn(mock(CloudBlobContainer.class));
        when(mockBlobClient.getContainerReference(anyString()).getBlockBlobReference(anyString())).thenReturn(mockBlockBlob);
        when(mockBlockBlob.getProperties()).thenReturn(mockBlobProperties);
        when(mockBlobProperties.getLastModified()).thenReturn(new Date());

        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Invoking the run method of BlobStorageTriggerFunction
        function.run(blob, filepath, mockContext);

        // Verify that downloadAttributes() is called
        verify(mockBlockBlob, times(1)).downloadAttributes();
    }

    @Test
    public void testBlobAttributesOlderThanThreshold() throws Exception {
        // Mocking blob and filepath
        String blob = "Sample Blob Data";
        String filepath = "test_blob.txt";

        // Mocking AzureWebJobsStorage and containerName environment variables
        System.setProperty("AZURE_STORAGE_CONTAINER", "test_container");
        System.setProperty("AzureWebJobsStorage", "test_connection_string");

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());

        // Mocking CloudBlobClient and CloudBlockBlob
        CloudBlobClient mockBlobClient = mock(CloudBlobClient.class);
        CloudBlockBlob mockBlockBlob = mock(CloudBlockBlob.class);
        BlobProperties mockBlobProperties = mock(BlobProperties.class);

        when(function.getCloudBlobClient()).thenReturn(mockBlobClient);
        when(mockBlobClient.getContainerReference(anyString())).thenReturn(mock(CloudBlobContainer.class));
        when(mockBlobClient.getContainerReference(anyString()).getBlockBlobReference(anyString())).thenReturn(mockBlockBlob);
        when(mockBlockBlob.getProperties()).thenReturn(mockBlobProperties);
        // Setting last modified time older than threshold (2 minutes)
        when(mockBlobProperties.getLastModified()).thenReturn(new Date(System.currentTimeMillis() - 2 * 60 * 1000));

        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Invoking the run method of BlobStorageTriggerFunction
        function.run(blob, filepath, mockContext);

        // Verify that downloadAttributes() is called
        verify(mockBlockBlob, times(1)).downloadAttributes();
        // Verify that logger logs info message for skipping blob
        verify(mockContext.getLogger(), times(1)).info(contains("Skipping blob"));
    }

    @Test
    public void testParseBlobDir() {
        // Mocking blobUrl with a sample URL
        String blobUrl = "https://testaccount.blob.core.windows.net/testcontainer/testdirectory/testfile.txt";

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = new BlobStorageTriggerFunction();

        // Invoking the parseBlobDir method
        String parsedDir = function.parseBlobDir(blobUrl);

        // Verifying the parsed directory
        assertEquals("testdirectory", parsedDir);
    }

    @Test
    public void testGetFileName() {
        // Mocking blobUrl with a sample URL
        String blobUrl = "https://testaccount.blob.core.windows.net/testcontainer/testdirectory/testfile.txt";

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = new BlobStorageTriggerFunction();

        // Invoking the getFileName method
        String fileName = function.getFileName(blobUrl);

        // Verifying the extracted file name
        assertEquals("testfile.txt", fileName);
    }

    @Test
    public void testGetContainerNameWithHttpPrefix() {
        // Mocking blobUrl with a sample URL with HTTP prefix
        String blobUrl = "https://testaccount.blob.core.windows.net/testcontainer/testdirectory/testfile.txt";

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = new BlobStorageTriggerFunction();

        // Invoking the getContainerName method
        String containerName = function.getContainerName(blobUrl);

        // Verifying the extracted container name
        assertEquals("testcontainer", containerName);
    }

    @Test
    public void testGetContainerNameWithAbfsPrefix() {
        // Mocking blobUrl with a sample URL with ABFS prefix
        String blobUrl = "abfs://testaccount.blob.core.windows.net/testcontainer/testdirectory/testfile.txt";

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = new BlobStorageTriggerFunction();

        // Invoking the getContainerName method
        String containerName = function.getContainerName(blobUrl);

        // Verifying the extracted container name
        assertEquals("testcontainer", containerName);
    }

    @Test
    public void testGetContainerNameInvalidBlobUrl() {
        // Mocking blobUrl with an invalid URL
        String blobUrl = "invalidUrl";

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = new BlobStorageTriggerFunction();

        // Invoking the getContainerName method
        String containerName = function.getContainerName(blobUrl);

        // Verifying the extracted container name (should be empty)
        assertTrue(containerName.isEmpty());
    }


    @Test
    public void testDatabaseQuery() throws SQLException {
        // Mocking environment variables
        System.setProperty("EFF_PGSQL_SA_CS", "test_connection_string");

        // Mocking a sample query result
        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockResultSet.next()).thenReturn(true).thenReturn(false); // Mocking one row of result
        when(mockResultSet.getString("json_data")).thenReturn("{\"blob_path\": \"test_path\", \"jobname\": \"test_job\"}");
        when(mockResultSet.getString("uniqueid")).thenReturn("123");
        when(mockResultSet.getString("jobid")).thenReturn("456");

        // Mocking Connection, PreparedStatement, and ResultSet
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

        // Mocking PGDbConnection class
        BlobStorageTriggerFunction.PGDbConnection mockPGDbConnection = spy(new BlobStorageTriggerFunction.PGDbConnection("test_connection_string"));
        doReturn(mockConnection).when(mockPGDbConnection).getConnection();

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());

        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Invoking the run method of BlobStorageTriggerFunction
        function.run("Sample Blob Data", "test_blob.txt", mockContext);

        // Verifying that the PreparedStatement is created and executed with the correct query
        verify(mockConnection, times(1)).prepareStatement(startsWith("select json_data::text as json"));

        // Verifying that the retrieved data is processed and Databricks job is triggered
        verify(mockContext.getLogger(), times(1)).info(startsWith("Triggering Databricks Job inside"));

        // Verifying that the logger logs info message for skipping blob in case of missing jobId
        verify(mockContext.getLogger(), never()).info(startsWith("Skipping blob for"));
    }

    @Test
    public void testDatabaseInsertion() throws SQLException {
        // Mocking environment variables
        System.setProperty("EFF_PGSQL_SA_CS", "test_connection_string");

        // Mocking Connection and PreparedStatement
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        // Mocking PGDbConnection class
        BlobStorageTriggerFunction.PGDbConnection mockPGDbConnection = spy(new BlobStorageTriggerFunction.PGDbConnection("test_connection_string"));
        doReturn(mockConnection).when(mockPGDbConnection).getConnection();

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());

        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Invoking the run method of BlobStorageTriggerFunction
        function.run("Sample Blob Data", "test_blob.txt", mockContext);

        // Verifying that the PreparedStatement is created and executed with the correct query
        verify(mockConnection, times(1)).prepareStatement(startsWith("INSERT INTO datafabric_logging.event_log"));

        // Verifying that the insertion query is executed
        verify(mockPreparedStatement, times(1)).executeUpdate();

        // Verifying that the logger logs an error message in case of SQLException
        verify(mockContext.getLogger(), never()).severe(anyString());
    }

    @Test
    public void testTriggerDBJob() throws Exception {
        // Mocking params and messageJson
        Map<String, Object> params = new HashMap<>();
        params.put("jar_params", new HashMap<>()); // Mocking jar_params

        Map<String, Object> messageJson = new HashMap<>();

        // Mocking HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.body()).thenReturn("{\"run_id\": \"123456\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockHttpResponse);

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());
        doReturn(mockHttpClient).when(function).getHttpClient(anyBoolean());

        // Invoking the triggerDBJob method
        String runId = function.triggerDBJob(params, messageJson);

        // Verifying the runId returned by the method
        assertEquals("123456", runId);
    }

    @Test
    public void testTriggerDBJobWithErrorResponse() throws Exception {
        // Mocking params and messageJson
        Map<String, Object> params = new HashMap<>();
        params.put("jar_params", new HashMap<>()); // Mocking jar_params

        Map<String, Object> messageJson = new HashMap<>();

        // Mocking HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockHttpResponse = mock(HttpResponse.class);
        when(mockHttpResponse.body()).thenReturn("{\"error\": \"Job execution failed\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockHttpResponse);

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());
        doReturn(mockHttpClient).when(function).getHttpClient(anyBoolean());

        // Invoking the triggerDBJob method
        String runId = function.triggerDBJob(params, messageJson);

        // Verifying that runId is -1 due to error response
        assertEquals("-1", runId);
    }

    @Test
    public void testDatabaseConnectionFailure() {
        // Mocking PGCS_DBW
        System.setProperty("EFF_PGSQL_SA_CS", "invalid_connection_string");

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());

        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Invoking the run method with database connection failure
        function.run("blob", "filepath", mockContext);

        // Verifying that logger logs a severe message for database connection failure
        verify(mockContext.getLogger(), times(1)).severe(contains("Failed to connect to the database"));
    }

    @Test
    public void testHttpRequestFailure() throws Exception {
        // Mocking HttpClient to throw an exception
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new RuntimeException("HTTP request failed"));

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());
        doReturn(mockHttpClient).when(function).getHttpClient(anyBoolean());

        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Invoking the run method with HTTP request failure
        function.run("blob", "filepath", mockContext);

        // Verifying that logger logs a severe message for HTTP request failure
        verify(mockContext.getLogger(), times(1)).severe(contains("HTTP request failed"));
    }

    @Test
    public void testSQLExceptionHandling() throws Exception {
        // Mocking PGCS_DBW
        System.setProperty("EFF_PGSQL_SA_CS", "valid_connection_string");

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = spy(new BlobStorageTriggerFunction());

        // Mocking SQLException while inserting data
        doThrow(new SQLException("SQL exception occurred")).when(function).insertData(anyString(), anyString(), anyString(), anyString(), anyString());

        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Invoking the run method with SQLException
        function.run("blob", "filepath", mockContext);

        // Verifying that logger logs a severe message for SQL exception
        verify(mockContext.getLogger(), times(1)).severe(contains("SQL exception occurred"));
    }

    @Test
    public void testInvalidInputHandling() {
        // Mocking ExecutionContext
        when(mockContext.getLogger()).thenReturn(mock(Logger.class));

        // Creating a mock instance of BlobStorageTriggerFunction
        BlobStorageTriggerFunction function = new BlobStorageTriggerFunction();

        // Invoking the run method with empty filepath
        function.run("blob", "", mockContext);

        // Verifying that logger logs a severe message for empty payload
        verify(mockContext.getLogger(), times(1)).severe(contains("Empty Payload"));
    }
}

