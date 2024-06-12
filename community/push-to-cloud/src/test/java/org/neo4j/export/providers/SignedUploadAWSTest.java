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
import static com.github.tomakehurst.wiremock.client.WireMock.binaryEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_OK;
import static wiremock.org.hamcrest.CoreMatchers.containsString;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.UploadCommand;
import org.neo4j.export.util.ExportTestUtilities;
import org.neo4j.export.util.IOCommon;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestDirectorySupportExtension.class)
@Neo4jLayoutExtension
public class SignedUploadAWSTest {

    private int wiremockServerPort;
    private String wireMockServerAddress;
    private static final String DBNAME = "neo4j";

    @Inject
    TestDirectory directory;

    private WireMockServer wireMockServer;

    private Path dump;

    private long storeSize;
    private ExecutionContext ctx;

    private long dumpFileSize;
    private String signedLinks[];

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
        signedLinks = new String[] {
            String.format("%s/signed1", wireMockServerAddress),
            String.format("%s/signed2", wireMockServerAddress),
            String.format("%s/signed3", wireMockServerAddress)
        };
        WireMock.configureFor("localhost", wiremockServerPort);
    }

    @AfterEach
    public void tearDownEach() {
        wireMockServer.resetAll();
        wireMockServer.stop();
    }

    @Test
    public void testAWSUploadHappyPathMultiPart() throws IOException {
        SignedUploadAWS signedUploadAWS =
                new SignedUploadAWS(signedLinks, "uploadID", signedLinks.length, ctx, "bolt://localhost");
        long chunkSize = signedUploadAWS.getChunkSize(dumpFileSize);
        UploadCommand.Source source = new UploadCommand.Source(ctx.fs(), dump, storeSize);
        byte[][] chunks = new byte[signedLinks.length][];
        setUpRequestChunks(chunkSize, chunks);
        wireMockServer.stubFor(uploadRequest().willReturn(successfulInitiateResponse()));
        signedUploadAWS.copy(false, source);
        verifyChunks(chunks);
    }

    @Test
    public void testAWSUploadWithRetryReUploadsCorrectChunk() throws IOException {

        SignedUploadAWS signedUploadAWS =
                new SignedUploadAWS(signedLinks, "uploadID", signedLinks.length, ctx, "bolt://localhost", millis -> {});
        long chunkSize = signedUploadAWS.getChunkSize(dumpFileSize);
        UploadCommand.Source source = new UploadCommand.Source(ctx.fs(), dump, storeSize);
        byte[][] chunks = new byte[signedLinks.length][];
        setUpRequestChunks(chunkSize, chunks);
        wireMockServer.stubFor(uploadRequest()
                .inScenario("Failed Upload")
                .whenScenarioStateIs(STARTED)
                .willReturn(failedInitiateResponse())
                .willSetStateTo("Retry Upload"));

        wireMockServer.stubFor(uploadRequest()
                .inScenario("Failed Upload")
                .whenScenarioStateIs("Retry Upload")
                .willReturn(successfulInitiateResponse()));
        signedUploadAWS.copy(false, source);

        wireMockServer.verify(
                exactly(2), putRequestedFor(urlEqualTo("/signed1")).withRequestBody(binaryEqualTo(chunks[0])));
    }

    @Test
    public void testAWSUploadWithRetryReUploadsCorrectChunkInMiddle() throws IOException {

        // Because there are only 3 numbers in computer science 0, 1 and n.
        SignedUploadAWS signedUploadAWS =
                new SignedUploadAWS(signedLinks, "uploadID", signedLinks.length, ctx, "bolt://localhost", millis -> {});
        long chunkSize = signedUploadAWS.getChunkSize(dumpFileSize);
        UploadCommand.Source source = new UploadCommand.Source(ctx.fs(), dump, storeSize);
        byte[][] chunks = new byte[signedLinks.length][];
        setUpRequestChunks(chunkSize, chunks);

        wireMockServer.stubFor(uploadRequest().willReturn(successfulInitiateResponse()));
        wireMockServer.stubFor(uploadRequest2()
                .inScenario("Failed Upload")
                .whenScenarioStateIs(STARTED)
                .willReturn(failedInitiateResponse())
                .willSetStateTo("Retry Upload"));

        wireMockServer.stubFor(uploadRequest2()
                .inScenario("Failed Upload")
                .whenScenarioStateIs("Retry Upload")
                .willReturn(successfulInitiateResponse()));
        signedUploadAWS.copy(false, source);

        wireMockServer.verify(
                exactly(2), putRequestedFor(urlEqualTo("/signed2")).withRequestBody(binaryEqualTo(chunks[1])));

        wireMockServer.verify(
                exactly(1), putRequestedFor(urlEqualTo("/signed1")).withRequestBody(binaryEqualTo(chunks[0])));

        wireMockServer.verify(
                exactly(1), putRequestedFor(urlEqualTo("/signed3")).withRequestBody(binaryEqualTo(chunks[2])));
    }

    @Test
    public void testCorrectlyErrorsAfterFiveAttempts() throws java.io.IOException {
        SignedUploadAWS signedUploadAWS =
                new SignedUploadAWS(signedLinks, "uploadID", signedLinks.length, ctx, "bolt://localhost", millis -> {});
        UploadCommand.Source source = new UploadCommand.Source(ctx.fs(), dump, storeSize);
        byte[][] chunks = new byte[signedLinks.length][];
        long chunkSize = signedUploadAWS.getChunkSize(dumpFileSize);
        setUpRequestChunks(chunkSize, chunks);
        wireMockServer.stubFor(uploadRequest().willReturn(failedInitiateResponse()));

        ExportTestUtilities.assertThrows(
                CommandFailedException.class,
                containsString(
                        "Failed to upload part to multipart url after 5 retries. Please check your Internet connection and try again."),
                () -> signedUploadAWS.copy(false, source));

        wireMockServer.verify(
                exactly(5), putRequestedFor(urlEqualTo("/signed1")).withRequestBody(binaryEqualTo(chunks[0])));
    }

    private void setUpRequestChunks(long chunkSize, byte[][] chunks) throws IOException {
        for (int i = 0; i < signedLinks.length - 1; i++) {
            chunks[i] = getNBytesFromFile((int) chunkSize, i * chunkSize);
        }
        // Last chunk is smaller than the rest
        chunks[signedLinks.length - 1] = getLastBytesFromFile((signedLinks.length - 1) * chunkSize);
    }

    private void verifyChunks(byte[][] chunks) {
        for (int i = 0; i < signedLinks.length; i++) {
            wireMockServer.verify(
                    exactly(1),
                    putRequestedFor(urlEqualTo("/signed" + (i + 1))).withRequestBody(binaryEqualTo(chunks[i])));
        }
    }

    private byte[] getNBytesFromFile(int n, long position) throws IOException {
        BufferedInputStream is = new BufferedInputStream(new FileInputStream(dump.toFile()));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOCommon.safeSkip(is, position);
        IOUtils.copyRange(is, n, baos);
        is.close();
        return baos.toByteArray();
    }

    private byte[] getLastBytesFromFile(long position) throws IOException {
        BufferedInputStream is = new BufferedInputStream(new FileInputStream(dump.toFile()));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOCommon.safeSkip(is, position);
        IOUtils.copy(is, baos);
        is.close();
        return baos.toByteArray();
    }

    private MappingBuilder uploadRequest() {
        return put(urlMatching("/signed([0-9]*)"));
    }

    private MappingBuilder uploadRequest2() {
        return put(urlEqualTo("/signed2"));
    }

    private ResponseDefinitionBuilder successfulInitiateResponse() {
        return aResponse().withStatus(HTTP_OK);
    }

    private ResponseDefinitionBuilder failedInitiateResponse() {
        return aResponse().withStatus(HTTP_BAD_GATEWAY);
    }
}
