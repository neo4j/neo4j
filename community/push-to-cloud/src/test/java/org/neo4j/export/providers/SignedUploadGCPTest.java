/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.export.providers;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static wiremock.org.hamcrest.CoreMatchers.containsString;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Path;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.CommandResponseHandler;
import org.neo4j.export.UploadCommand;
import org.neo4j.export.providers.SignedUploadGCP.ProgressListenerFactory;
import org.neo4j.export.util.ExportTestUtilities;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestDirectorySupportExtension.class)
@Neo4jLayoutExtension
public class SignedUploadGCPTest {

    private int wiremockServerPort;
    private String wireMockServerAddress;
    private String wiremockInitiateUrl;
    private String wiremockUploadUrl;
    private static final String DBNAME = "neo4j";
    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    @Inject
    TestDirectory directory;

    private WireMockServer wireMockServer;

    private Path dump;

    private long storeSize;
    private ExecutionContext ctx;

    private long dumpFileSize;

    @Inject
    private Neo4jLayout neo4jLayout;

    @BeforeAll
    public void setup() {
        Path homeDir = directory.homePath();
        Path confPath = directory.directory("conf");
        Path dumpDir = directory.directory("dumps");
        dump = dumpDir.resolve(DBNAME + ".dump");
        ExportTestUtilities.prepareDatabase(neo4jLayout.databaseLayout(DBNAME));
        PrintStream nullOutputStream = new PrintStream(NullOutputStream.nullOutputStream());
        ctx = new ExecutionContext(homeDir, confPath, nullOutputStream, nullOutputStream, directory.getFileSystem());
        ExportTestUtilities.createDump(homeDir, confPath, dumpDir, ctx.fs(), DBNAME);
        wireMockServer = new WireMockServer(options().dynamicPort().notifier(new ConsoleNotifier(false)));
    }

    @BeforeEach
    public void setupEach() throws IOException {
        wireMockServer.start();
        storeSize = UploadCommand.readSizeFromArchiveMetaData(ctx, dump);
        dumpFileSize = ctx.fs().getFileSize(dump);
        wiremockServerPort = wireMockServer.port();
        wireMockServerAddress = "http://localhost:" + wiremockServerPort;
        wiremockInitiateUrl = wireMockServerAddress + "/initiate";
        wiremockUploadUrl = wireMockServerAddress + "/upload";
        WireMock.configureFor("localhost", wiremockServerPort);
    }

    @AfterEach
    public void tearDownEach() {
        wireMockServer.stop();
    }

    @Test
    public void testGCPUploadHappyPath() {
        ControlledProgressListener progressListener = new ControlledProgressListener();
        SignedUploadGCP.ProgressListenerFactory progressListenerFactory = (name, length) -> progressListener;

        wireMockServer.stubFor(initiateRequest().willReturn(successfulInitiateResponse("/upload")));
        wireMockServer.stubFor(resumeUpload().willReturn(aResponse().withStatus(HTTP_OK)));
        SignedUploadGCP gcpSignedUpload = getGcpSignedUpload(progressListenerFactory);
        UploadCommand.Source source = new UploadCommand.Source(ctx.fs(), dump, storeSize);
        gcpSignedUpload.copy(true, source);

        verify(postRequestedFor(urlEqualTo("/initiate")));
        verify(putRequestedFor(urlEqualTo("/upload")));
        assertTrue(progressListener.closeCalled);
        assertEquals(dumpFileSize, progressListener.progress);
    }

    @Test
    public void shouldHandleResumableFailureWhileUploading() {
        ControlledProgressListener progressListener = new ControlledProgressListener();
        SignedUploadGCP.ProgressListenerFactory progressListenerFactory = (name, length) -> progressListener;

        wireMockServer.stubFor(initiateRequest().willReturn(successfulInitiateResponse("/upload")));
        wireMockServer.stubFor(resumeUpload().willReturn(aResponse().withStatus(HTTP_TOO_MANY_REQUESTS)));
        SignedUploadGCP gcpSignedUpload = getGcpSignedUpload(progressListenerFactory);
        UploadCommand.Source source = new UploadCommand.Source(ctx.fs(), dump, storeSize);

        ExportTestUtilities.assertThrows(
                CommandFailedException.class,
                containsString("You can re-try using the existing dump by running this command"),
                () -> gcpSignedUpload.copy(true, source));

        verify(postRequestedFor(urlEqualTo("/initiate")));
        verify(putRequestedFor(urlEqualTo("/upload")));
    }

    @Test
    public void shouldHandleServerErrorWhileUploading() {
        ControlledProgressListener progressListener = new ControlledProgressListener();
        SignedUploadGCP.ProgressListenerFactory progressListenerFactory = (name, length) -> progressListener;
        wireMockServer.stubFor(initiateRequest().willReturn(successfulInitiateResponse("/upload")));
        wireMockServer.stubFor(resumeUpload()
                .willReturn(aResponse().withStatus(HTTP_INTERNAL_ERROR))
                .inScenario("test")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("keepResuming"));

        wireMockServer.stubFor(keepResuming()
                .willReturn(aResponse().withStatus(HTTP_INTERNAL_ERROR))
                .inScenario("test")
                .whenScenarioStateIs("keepResuming"));

        SignedUploadGCP gcpSignedUpload = getGcpSignedUpload(progressListenerFactory);
        UploadCommand.Source source = new UploadCommand.Source(ctx.fs(), dump, storeSize);

        ExportTestUtilities.assertThrows(
                CommandFailedException.class,
                containsString("Unexpected response code"),
                () -> gcpSignedUpload.copy(true, source));

        verify(postRequestedFor(urlEqualTo("/initiate")));
        verify(putRequestedFor(urlEqualTo("/upload")));
    }

    @Test
    public void shouldHandleInitiateUploadFailure() {
        ControlledProgressListener progressListener = new ControlledProgressListener();
        SignedUploadGCP.ProgressListenerFactory progressListenerFactory = (name, length) -> progressListener;

        wireMockServer.stubFor(initiateRequest().willReturn(aResponse().withStatus(HTTP_INTERNAL_ERROR)));
        SignedUploadGCP gcpSignedUpload = getGcpSignedUpload(progressListenerFactory);
        UploadCommand.Source source = new UploadCommand.Source(ctx.fs(), dump, storeSize);

        ExportTestUtilities.assertThrows(
                CommandFailedException.class,
                containsString("Unexpected response"),
                () -> gcpSignedUpload.copy(true, source));

        verify(postRequestedFor(urlEqualTo("/initiate")));
    }

    @Test
    public void shouldGetCorrectVersionedEndpoint() {
        SignedUploadGCP signedUploadGCP =
                new SignedUploadGCP(null, "https://my_signed_url", ctx, "bolt://uri", null, null, null);
        URL endpoint = signedUploadGCP.getCorrectVersionedEndpoint();
        assertEquals("https://my_signed_url", endpoint.toString());
    }

    @Test
    public void shouldDefaultToList() {
        SignedUploadGCP signedUploadGCP = new SignedUploadGCP(
                new String[] {"https://my_list_signed_url"},
                "https://my_signed_url",
                ctx,
                "bolt://uri",
                null,
                null,
                null);
        URL endpoint = signedUploadGCP.getCorrectVersionedEndpoint();
        assertEquals("https://my_list_signed_url", endpoint.toString());
    }

    @Test
    void shouldReturnTrueWhenFileExists() throws IOException {
        SignedUploadGCP signedUploadGCP = getGcpSignedUpload(null);
        String error = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>AccessDenied</Code><Message>Access denied."
                + "</Message><Details>hello@hello.iam.gserviceaccount.com does not have storage.objects.delete "
                + "access to the Google Cloud Storage object.</Details></Error>";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(error.getBytes());
        assertTrue(signedUploadGCP.canSkipToImport(byteArrayInputStream));
    }

    @Test
    void shouldProduceErrorWhenNotPermissionDenied() throws IOException {
        SignedUploadGCP signedUploadGCP = getGcpSignedUpload(null);
        String error = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>AccessDenied</Code><Message>Access denied."
                + "</Message><Details>Unexpected stuff</Details></Error>";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(error.getBytes());
        assertFalse(signedUploadGCP.canSkipToImport(byteArrayInputStream));
    }

    @Test
    void shouldThrowErrorParsingXEEVulnerableContent() throws IOException {
        SignedUploadGCP signedUploadGCP = getGcpSignedUpload(null);
        String error = "<?xml version='1.0' encoding='UTF-8'?>"
                + "<!DOCTYPE foo [ <!ENTITY xxe SYSTEM \"file:///etc/ntp.conf\"> ]>"
                + "<Error><Code>&xxe;</Code><Message>Access denied."
                + "</Message><Details>hello@hello.iam.gserviceaccount.com does not have storage.objects.delete "
                + "access to the Google Cloud Storage object.</Details></Error>";

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(error.getBytes());
        ExportTestUtilities.assertThrows(
                IOException.class,
                containsString("Encountered invalid response from cloud import location"),
                () -> signedUploadGCP.canSkipToImport(byteArrayInputStream));
    }

    private MappingBuilder initiateRequest() {
        return post(urlEqualTo("/initiate"))
                .withHeader("x-goog-resumable", equalTo("start"))
                .withHeader("Content-Type", equalTo(""))
                .withHeader("Content-Length", equalTo("0"));
    }

    private MappingBuilder resumeRequest() {
        return post(urlEqualTo("/upload"))
                .withHeader("Content-Length", equalTo("0"))
                .withHeader("Content-Length", containing("bytes */"));
    }

    private ResponseDefinitionBuilder successfulInitiateResponse(String uploadPath) {
        return aResponse().withStatus(HTTP_CREATED).withHeader("Location", wireMockServerAddress + uploadPath);
    }

    private MappingBuilder resumeUpload() {
        return put(urlEqualTo("/upload")).withHeader("Content-Length", equalTo(String.valueOf(dumpFileSize)));
    }

    private MappingBuilder keepResuming() {
        return put(urlEqualTo("/upload")).withHeader("Content-Length", equalTo("0"));
    }

    private static class ControlledProgressListener extends ProgressListener.Adapter {
        long progress;
        boolean closeCalled;

        @Override
        public void add(long progress) {
            this.progress += progress;
        }

        @Override
        public void close() {
            closeCalled = true;
        }

        @Override
        public void failed(Throwable e) {
            throw new UnsupportedOperationException("Should not be called");
        }
    }

    private SignedUploadGCP getGcpSignedUpload(ProgressListenerFactory progressListenerFactory) {
        return new SignedUploadGCP(
                null,
                wiremockInitiateUrl,
                ctx,
                "bolt://uri",
                progressListenerFactory,
                Thread::sleep,
                new CommandResponseHandler(ctx));
    }
}
