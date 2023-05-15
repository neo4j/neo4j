/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

package org.neo4j.export;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.output.NullOutputStream.nullOutputStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.BasicCredentials;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.ResourceBundle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.AuraResponse.SignedURIBody;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.SystemNanoClock;
import picocli.CommandLine;
import wiremock.com.fasterxml.jackson.core.JsonProcessingException;
import wiremock.com.fasterxml.jackson.databind.ObjectMapper;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestDirectorySupportExtension.class)
@Neo4jLayoutExtension
public class UploadCommandTest {

    static final String ERROR_REASON_UNSUPPORTED_INDEXES = "LegacyIndexes";
    private static final int MOCK_SERVER_PORT = 8080;
    private static final String DBNAME = "neo4j";
    private static final String MOCK_BASE_URL = "http://localhost:" + MOCK_SERVER_PORT;
    private static final String SOME_EXAMPLE_BOLT_URI = "bolt+routing://database_id.databases.neo4j.io";
    private static final String STATUS_POLLING_PASSED_FIRST_CALL = "Passed first";
    private static final String STATUS_POLLING_PASSED_SECOND_CALL = "Passed second";
    private static final String INITATE_PRESIGNED_UPLOAD_LOCATION = "/initiate-presigned";
    private static final String UPLOAD_PRESIGNED_LOCATION = "/upload-presigned";
    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    WireMockServer wireMockServer;
    ExecutionContext ctx;

    @Inject
    TestDirectory directory;

    private Path dumpDir;

    private Path homeDir;
    private Path dump;
    private Path confPath;

    private long dbFullSize;

    @Inject
    private Neo4jLayout neo4jLayout;

    @BeforeEach
    public void setupEach() {
        wireMockServer = new WireMockServer(options().port(MOCK_SERVER_PORT).notifier(new ConsoleNotifier(false)));
        WireMock.configureFor("localhost", MOCK_SERVER_PORT);
        wireMockServer.start();
    }

    @BeforeAll
    public void setup() throws IOException {
        homeDir = directory.homePath();
        confPath = directory.directory("conf");
        Path configDir = directory.directory("config-dir");
        Path configFile = configDir.resolve("neo4j.conf");
        dumpDir = directory.directory("dumps");
        ExportTestUtilities.prepareDatabase(neo4jLayout.databaseLayout(DBNAME));
        Files.createFile(configFile);
        PrintStream nullOutputStream = new PrintStream(nullOutputStream());
        ctx = new ExecutionContext(homeDir, confPath, nullOutputStream, nullOutputStream, directory.getFileSystem());
        dump = dumpDir.resolve(DBNAME + ".dump");
        ExportTestUtilities.createDump(homeDir, confPath, dumpDir, fs, DBNAME);
        dbFullSize = UploadCommand.readSizeFromDumpMetaData(ctx, dump);
    }

    @AfterEach
    public void teardown() {
        wireMockServer.stop();
    }

    @Test
    public void happyPathUploadCommandTest() {

        String authResponse = "token";
        createHappyPathWireMockStubs(authResponse);

        wireMockServer.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authResponse))
                .willReturn(firstSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(STATUS_POLLING_PASSED_FIRST_CALL));

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI("username", "password", false));

        String[] args = {
            DBNAME,
            "--from-path",
            dumpDir.toAbsolutePath().toString(),
            "--to-uri",
            SOME_EXAMPLE_BOLT_URI,
            "--to-password",
            "password"
        };
        assertDoesNotThrow(() -> new CommandLine(command).execute(args));

        verify(postRequestedFor(urlEqualTo("/import/auth")));
        verify(postRequestedFor(urlEqualTo("/import/size")));
        verify(postRequestedFor(urlEqualTo("/import")));
        verify(postRequestedFor(urlEqualTo("/import/upload-complete")));
        verify(getRequestedFor(urlEqualTo("/import/status")));
        verify(postRequestedFor(urlEqualTo(INITATE_PRESIGNED_UPLOAD_LOCATION)));
        verify(putRequestedFor(urlEqualTo(UPLOAD_PRESIGNED_LOCATION)));
    }

    @Test
    public void shouldHandleUploadStartTimeoutFailure() {

        String authResponse = "token";
        createHappyPathWireMockStubs(authResponse);

        wireMockServer.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authResponse))
                .willReturn(secondSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(STATUS_POLLING_PASSED_FIRST_CALL));

        wireMockServer.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authResponse))
                .willReturn(secondSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_FIRST_CALL)
                .willSetStateTo(STATUS_POLLING_PASSED_FIRST_CALL));

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        SystemNanoClock clockMock = mock(SystemNanoClock.class);
        auraClientBuilder.withClock(clockMock);

        when(clockMock.millis()).thenReturn(0L).thenReturn(120 * 1000L);
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI("username", "password", false));

        String[] args = {
            DBNAME,
            "--from-path",
            dumpDir.toAbsolutePath().toString(),
            "--to-uri",
            SOME_EXAMPLE_BOLT_URI,
            "--to-password",
            "password"
        };
        CommandLine.populateCommand(command, args);

        var exception = assertThrows(CommandFailedException.class, () -> command.execute());
        assertThat(exception.getMessage()).contains("Timed out waiting for database load to start");

        verify(postRequestedFor(urlEqualTo("/import/auth")));
        verify(postRequestedFor(urlEqualTo("/import/size")));
        verify(postRequestedFor(urlEqualTo("/import")));
        verify(postRequestedFor(urlEqualTo("/import/upload-complete")));
        verify(getRequestedFor(urlEqualTo("/import/status")));
        verify(postRequestedFor(urlEqualTo(INITATE_PRESIGNED_UPLOAD_LOCATION)));
        verify(putRequestedFor(urlEqualTo(UPLOAD_PRESIGNED_LOCATION)));
    }

    @Test
    void shouldHandleConflictOnTriggerImportAfterUpload() {
        // given
        String authResponse = "token";
        createHappyPathWireMockStubs(authResponse);

        wireMockServer.stubFor(
                triggerImportRequest(authResponse).willReturn(aResponse().withStatus(HTTP_CONFLICT)));
        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);

        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI("username", "password", false));

        String[] args = {
            DBNAME,
            "--from-path",
            dumpDir.toAbsolutePath().toString(),
            "--to-uri",
            SOME_EXAMPLE_BOLT_URI,
            "--to-password",
            "password"
        };
        CommandLine.populateCommand(command, args);

        var exception = assertThrows(CommandFailedException.class, () -> command.execute());
        assertThat(exception.getMessage())
                .contains(
                        "The target database contained data and consent to overwrite the data was not given. Aborting");
    }

    @Test
    public void shouldHandleFailedImport() throws JsonProcessingException {

        String authResponse = "token";
        createHappyPathWireMockStubs(authResponse);

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI("username", "password", false));

        AuraClient.StatusBody statusBody = new AuraClient.StatusBody();
        statusBody.Status = "loading failed";
        String errorMessage = "The uploaded dump file contains deprecated indexes, "
                + "which we are unable to import in the current version of Neo4j Aura. "
                + "Please upgrade to the recommended index provider.";
        String errorUrl = "https://aura.support.neo4j.com/";
        statusBody.Error = new AuraClient.ErrorBody(errorMessage, ERROR_REASON_UNSUPPORTED_INDEXES, errorUrl);
        ObjectMapper mapper = new ObjectMapper();

        wireMockServer.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authResponse))
                .willReturn(aResponse()
                        .withBody(mapper.writeValueAsString(statusBody))
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HTTP_OK))
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_FIRST_CALL));

        String[] args = {
            DBNAME,
            "--from-path",
            dumpDir.toAbsolutePath().toString(),
            "--to-uri",
            SOME_EXAMPLE_BOLT_URI,
            "--to-password",
            "password"
        };
        CommandLine.populateCommand(command, args);
        var exception = assertThrows(CommandFailedException.class, () -> command.execute());
        String message = exception.getMessage();
        assertTrue(message.contains(errorMessage));
        assertFalse(message.contains(ERROR_REASON_UNSUPPORTED_INDEXES));
        assertFalse(message.contains(".."));

        verify(postRequestedFor(urlEqualTo("/import/auth")));
        verify(postRequestedFor(urlEqualTo("/import/size")));
        verify(postRequestedFor(urlEqualTo("/import")));
        verify(postRequestedFor(urlEqualTo("/import/upload-complete")));
        verify(getRequestedFor(urlEqualTo("/import/status")));
        verify(postRequestedFor(urlEqualTo(INITATE_PRESIGNED_UPLOAD_LOCATION)));
        verify(putRequestedFor(urlEqualTo(UPLOAD_PRESIGNED_LOCATION)));
    }

    @Test
    void shouldHandleFailedImportStatusFromPreviousLoad() throws IOException, CommandFailedException {
        // given
        String authResponse = "token";
        createHappyPathWireMockStubs(authResponse);

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI("username", "password", false));

        // ...and

        AuraClient.StatusBody statusBody = new AuraClient.StatusBody();
        statusBody.Status = "loading failed";
        String errorMessage = "The uploaded dump file contains deprecated indexes, "
                + "which we are unable to import in the current version of Neo4j Aura. "
                + "Please upgrade to the recommended index provider.";
        String errorUrl = "https://aura.support.neo4j.com/";
        statusBody.Error = new AuraClient.ErrorBody(errorMessage, ERROR_REASON_UNSUPPORTED_INDEXES, errorUrl);
        ObjectMapper mapper = new ObjectMapper();

        wireMockServer.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authResponse))
                .willReturn(aResponse()
                        .withBody(mapper.writeValueAsString(statusBody))
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HTTP_OK))
                .inScenario("test")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(STATUS_POLLING_PASSED_FIRST_CALL));

        wireMockServer.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authResponse))
                .willReturn(firstSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_FIRST_CALL)
                .willSetStateTo(STATUS_POLLING_PASSED_SECOND_CALL));

        wireMockServer.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authResponse))
                .willReturn(secondSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_SECOND_CALL));

        // when
        String[] args = {
            DBNAME,
            "--from-path",
            dumpDir.toAbsolutePath().toString(),
            "--to-uri",
            SOME_EXAMPLE_BOLT_URI,
            "--to-password",
            "password"
        };
        int exitCode = new CommandLine(command).execute(args);

        // then
        assertEquals(0, exitCode);
        verify(postRequestedFor(urlEqualTo("/import/auth")));
        verify(postRequestedFor(urlEqualTo("/import"))
                .withRequestBody(matchingJsonPath("$.FullSize", equalTo(String.valueOf(dbFullSize)))));
        verify(postRequestedFor(urlEqualTo(INITATE_PRESIGNED_UPLOAD_LOCATION)));
        verify(putRequestedFor(urlEqualTo(UPLOAD_PRESIGNED_LOCATION)));
        verify(postRequestedFor(urlEqualTo("/import/upload-complete")));
    }

    private void createHappyPathWireMockStubs(String authResponse) {
        wireMockServer.stubFor(authenticationRequest().willReturn(successfulAuthorizationResponse("token")));
        wireMockServer.stubFor(sizeCheckTargetRequest("token").willReturn(successfulSizeCheckTargetResponse()));
        wireMockServer.stubFor(initiateUploadTargetRequest(authResponse)
                .willReturn(successfulInitiateUploadTargetResponse(INITATE_PRESIGNED_UPLOAD_LOCATION)));
        wireMockServer.stubFor(triggerImportRequest(authResponse).willReturn(successfulTriggerImportResponse()));
        wireMockServer.stubFor(firstStatusPollingRequest(authResponse));
        wireMockServer.stubFor(secondStatusPollingRequest(authResponse));
        wireMockServer.stubFor(initatePreSignedUpload().willReturn(successfulInitatePresignedResponse()));

        wireMockServer.stubFor(uploadToPreSignedUrl().willReturn(successfulUploadPresignedResponse()));
    }

    @Test
    public void shouldReadUsernameAndPasswordFromUserInput() {
        // given
        String username = "neo4j";
        String password = "abc";

        wireMockServer.stubFor(authenticationRequest().willReturn(successfulAuthorizationResponse("token")));

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI(username, password, false));

        // when
        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        new CommandLine(command).execute(args);

        // then
        verify(postRequestedFor(urlEqualTo("/import/auth")).withBasicAuth(new BasicCredentials(username, password)));
    }

    @Test
    public void shouldUseNeo4jAsDefaultUsernameIfUserHitsEnter() {
        // given
        wireMockServer.stubFor(authenticationRequest().willReturn(successfulAuthorizationResponse("token")));
        PushToCloudCLI pushToCloudCLI = mock(PushToCloudCLI.class);
        when(pushToCloudCLI.readLine(anyString(), anyString())).thenReturn("");
        String defaultUsername = "neo4j";
        String password = "super-secret-password";

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);

        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        UploadCommand command = new UploadCommand(
                ctx, auraClientBuilder, auraURLFactory, new UploadCommandTest.FakeUploadURLFactory(), pushToCloudCLI);

        // when
        String[] args = {
            DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI, "--to-password", password
        };
        new CommandLine(command).execute(args);

        // then
        verify(pushToCloudCLI).readLine("%s", format("Neo4j aura username (default: %s):", defaultUsername));
        verify(postRequestedFor(urlEqualTo("/import/auth")).withBasicAuth(new BasicCredentials("neo4j", password)));
    }

    @Test
    public void shouldAcceptPasswordViaArgAndPromptForUsername() throws CommandFailedException {
        // given
        String username = "neo4juserviacli";
        wireMockServer.stubFor(authenticationRequest().willReturn(successfulAuthorizationResponse("token")));

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);

        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI(username, "tomte", false));

        // when
        String[] args = {
            DBNAME, "--from-path", dumpDir.toString(), "--to-password", "pass", "--to-uri", SOME_EXAMPLE_BOLT_URI
        };
        new CommandLine(command).execute(args);

        // then
        verify(postRequestedFor(urlEqualTo("/import/auth")).withBasicAuth(new BasicCredentials(username, "pass")));
    }

    @Test
    public void shouldAcceptPasswordViaEnvAndPromptForUsername() {
        // given
        String username = "neo4juserviacli";

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);

        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);

        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        var environment = Map.of("NEO4J_USERNAME", "", "NEO4J_PASSWORD", "pass");

        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI(username, "tomte", false));

        // when
        new CommandLine(command)
                .setResourceBundle(new MapResourceBundle(environment))
                .execute(args);

        // then
        wireMockServer.verify(
                postRequestedFor(urlEqualTo("/import/auth")).withBasicAuth(new BasicCredentials(username, "pass")));
    }

    @Test
    public void shouldAcceptUsernameViaArgAndPromptForPassword() throws CommandFailedException {
        // given
        String username = "neo4j";
        String password = "abc";

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);

        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI(username, password, false));
        // when
        String[] args = {
            DBNAME, "--from-path", dumpDir.toString(), "--to-user", "user", "--to-uri", SOME_EXAMPLE_BOLT_URI
        };
        new CommandLine(command).execute(args);

        // then
        assertTrue(Files.exists(dump));
        verify(postRequestedFor(urlEqualTo("/import/auth")).withBasicAuth(new BasicCredentials("user", password)));
    }

    @Test
    public void shouldAcceptUsernameViaEnvAndPromptForPassword() {
        // given
        String username = "neo4jcliuser";
        String password = "abc";
        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);

        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI(username, password, false));
        // when
        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};

        var environment = Map.of("NEO4J_USERNAME", "user", "NEO4J_PASSWORD", "");
        new CommandLine(command)
                .setResourceBundle(new MapResourceBundle(environment))
                .execute(args);
        assertTrue(Files.exists(dump));
        verify(postRequestedFor(urlEqualTo("/import/auth")).withBasicAuth(new BasicCredentials("user", password)));
    }

    @Test
    public void shouldAcceptOnlyUsernameAndPasswordFromCli() throws CommandFailedException {
        // given
        String username = "neo4jcliuser";
        String password = "abc";

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);

        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI(username, password, false));

        // when
        String[] args = {
            DBNAME,
            "--from-path",
            dumpDir.toString(),
            "--to-user",
            "neo4jarg",
            "--to-password",
            "passcli",
            "--to-uri",
            SOME_EXAMPLE_BOLT_URI
        };
        new CommandLine(command).execute(args);

        verify(postRequestedFor(urlEqualTo("/import/auth")).withBasicAuth(new BasicCredentials("neo4jarg", "passcli")));
    }

    @Test
    public void shouldAcceptOnlyUsernameAndPasswordFromEnv() {
        // given
        String username = "neo4jcliuser";
        String password = "abc";

        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);

        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI(username, password, false));

        // when
        String[] args = {DBNAME, "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        var environment = Map.of("NEO4J_USERNAME", "neo4jenv", "NEO4J_PASSWORD", "passenv");
        new CommandLine(command)
                .setResourceBundle(new MapResourceBundle(environment))
                .execute(args);

        verify(postRequestedFor(urlEqualTo("/import/auth")).withBasicAuth(new BasicCredentials("neo4jenv", "passenv")));
    }

    @Test
    public void shouldFailOnDumpPointingToMissingFile() throws CommandFailedException {
        // given
        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        String[] args = {"otherdbname", "--from-path", dumpDir.toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI("neo4j", "abc", false));
        CommandLine.populateCommand(command, args);
        final var assertFailure = assertThatThrownBy(command::execute).isInstanceOf(CommandFailedException.class);
        assertFailure.hasMessageContaining("otherdbname.tar' do not exist");
        assertFailure.hasMessageContaining("otherdbname.dump");
    }

    @Test
    public void shouldFailOnWrongDumpPath() throws CommandFailedException {
        AuraURLFactory auraURLFactory = mock(AuraURLFactory.class);
        when(auraURLFactory.buildConsoleURI(any(), anyBoolean())).thenReturn(MOCK_BASE_URL);
        org.neo4j.export.AuraClient.AuraClientBuilder auraClientBuilder =
                new org.neo4j.export.AuraClient.AuraClientBuilder(ctx);
        String[] args = {DBNAME, "--from-path", dump.toAbsolutePath().toString(), "--to-uri", SOME_EXAMPLE_BOLT_URI};
        UploadCommand command = new UploadCommand(
                ctx,
                auraClientBuilder,
                auraURLFactory,
                new UploadCommandTest.FakeUploadURLFactory(),
                PushToCloudCLI.fakeCLI("neo4j", "abc", false));
        CommandLine.populateCommand(command, args);
        final var assertFailure = assertThatThrownBy(command::execute).isInstanceOf(CommandFailedException.class);
        assertFailure.hasMessageContaining("The provided source directory");
    }

    private ResponseDefinitionBuilder successfulAuthorizationResponse(String authorizationTokenResponse) {
        return aResponse().withStatus(HTTP_OK).withBody(format("{\"Token\":\"%s\"}", authorizationTokenResponse));
    }

    private MappingBuilder authenticationRequest() {
        return post(urlEqualTo("/import/auth"))
                .withHeader("Authorization", matching("^Basic .*"))
                .withHeader("Accept", equalTo("application/json"))
                .withHeader("Confirmed", equalTo("false"));
    }

    private MappingBuilder sizeCheckTargetRequest(String authorizationTokenResponse) {
        return post(urlEqualTo("/import/size"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .withRequestBody(equalToJson("{\"FullSize\": " + dbFullSize + "}"));
    }

    private ResponseDefinitionBuilder successfulSizeCheckTargetResponse() {
        return aResponse().withStatus(HTTP_OK);
    }

    private MappingBuilder initiateUploadTargetRequest(String authorizationTokenResponse) {
        return post(urlEqualTo("/import"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .withHeader("Accept", equalTo("application/json"))
                // We can't actually test the value that gets set in the header
                // because it comes from the jar's manifest which is not available at test time
                .withHeader("Neo4j-Version", matching(".*"));
    }

    private MappingBuilder initatePreSignedUpload() {
        return post(urlEqualTo(INITATE_PRESIGNED_UPLOAD_LOCATION))
                .withHeader("Content-Length", equalTo("0"))
                .withHeader("x-goog-resumable", equalTo("start"))
                .withHeader("Content-Type", equalTo(""));
    }

    private MappingBuilder uploadToPreSignedUrl() {
        return put(urlEqualTo(UPLOAD_PRESIGNED_LOCATION));
    }

    private ResponseDefinitionBuilder successfulInitiateUploadTargetResponse(String signedURIPath) {
        return aResponse()
                .withStatus(HTTP_ACCEPTED)
                .withBody(format(
                        "{\"SignedURI\":\"%s\", \"expiration_date\":\"Fri, 04 Oct 2019 08:21:59 GMT\"}",
                        MOCK_BASE_URL + signedURIPath));
    }

    private MappingBuilder triggerImportRequest(String authorizationTokenResponse) {
        return post(urlEqualTo("/import/upload-complete"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .withRequestBody(containing("Crc32"));
    }

    private ResponseDefinitionBuilder successfulTriggerImportResponse() {
        return aResponse().withStatus(HTTP_OK);
    }

    private MappingBuilder firstStatusPollingRequest(String authorizationTokenResponse) {
        return get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(firstSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(STATUS_POLLING_PASSED_FIRST_CALL);
    }

    private MappingBuilder secondStatusPollingRequest(String authorizationTokenResponse) {
        return get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(secondSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_FIRST_CALL);
    }

    private ResponseDefinitionBuilder firstSuccessfulDatabaseRunningResponse() {
        return aResponse().withBody("{\"Status\":\"loading\"}").withStatus(HTTP_OK);
    }

    private ResponseDefinitionBuilder secondSuccessfulDatabaseRunningResponse() {
        return aResponse().withBody("{\"Status\":\"running\"}").withStatus(HTTP_OK);
    }

    private ResponseDefinitionBuilder successfulInitatePresignedResponse() {
        return aResponse().withStatus(HTTP_CREATED).withHeader("Location", MOCK_BASE_URL + UPLOAD_PRESIGNED_LOCATION);
    }

    private ResponseDefinitionBuilder successfulUploadPresignedResponse() {
        return aResponse().withStatus(HTTP_OK);
    }

    private static class MapResourceBundle extends ResourceBundle {
        private final Map<String, String> entries;

        MapResourceBundle(Map<String, String> entries) {
            requireNonNull(entries);
            this.entries = entries;
        }

        @Override
        protected Object handleGetObject(String key) {
            requireNonNull(key);
            return entries.get(key);
        }

        @Override
        public Enumeration<String> getKeys() {
            return Collections.enumeration(entries.keySet());
        }
    }

    private class FakeUploadURLFactory implements UploadURLFactory {
        private final String signedURIPath = MOCK_BASE_URL + INITATE_PRESIGNED_UPLOAD_LOCATION;

        @Override
        public SignedUpload fromAuraResponse(SignedURIBody signedURIBody, ExecutionContext ctx, String boltURI) {
            return new SignedUploadGCP(
                    null,
                    signedURIPath,
                    ctx,
                    "bolt://uri",
                    (name, length) -> new ExportTestUtilities.ControlledProgressListener(),
                    millis -> {},
                    new CommandResponseHandler(ctx));
        }
    }
}
