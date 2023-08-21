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
package org.neo4j.export;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.io.OutputStream.nullOutputStream;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static wiremock.org.hamcrest.CoreMatchers.allOf;
import static wiremock.org.hamcrest.CoreMatchers.containsString;
import static wiremock.org.hamcrest.MatcherAssert.assertThat;
import static wiremock.org.hamcrest.Matchers.not;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.aura.AuraClient;
import org.neo4j.export.aura.AuraConsole;
import org.neo4j.export.aura.AuraJsonMapper;
import org.neo4j.export.util.ExportTestUtilities;
import org.neo4j.export.util.IOCommon;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import wiremock.com.fasterxml.jackson.databind.ObjectMapper;
import wiremock.org.hamcrest.CoreMatchers;
import wiremock.org.hamcrest.Matcher;

@TestDirectoryExtension
public class AuraClientTest {

    private static final int TEST_PORT = 8080;
    private static final String TEST_CONSOLE_URL = "http://localhost:" + TEST_PORT;
    private static final String STATUS_POLLING_PASSED_SECOND_CALL = "Passed second";
    private static final AuraClient.ProgressListenerFactory NO_OP_PROGRESS = (name, length) -> ProgressListener.NONE;
    private static final int HTTP_UNPROCESSABLE_ENTITY = 422;
    static final String ERROR_REASON_EXCEEDS_MAX_SIZE = "ImportExceedsMaxSize";

    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    WireMockServer wireMock;
    ExecutionContext ctx;

    @Inject
    TestDirectory directory;

    private static void assertThrows(
            Class<? extends Exception> exceptionClass, Matcher<String> message, ThrowingRunnable action) {
        try {
            action.run();
            fail("Should have failed");
        } catch (Exception e) {
            assertTrue(exceptionClass.isInstance(e));
            assertThat(e.getMessage(), message);
        }
    }

    @BeforeEach
    public void setup() {
        wireMock = new WireMockServer(TEST_PORT);
        WireMock.configureFor("localhost", TEST_PORT);
        wireMock.start();
        Path dir = directory.homePath();
        PrintStream out = new PrintStream(nullOutputStream());
        ctx = new ExecutionContext(dir, dir, out, out, fs);
    }

    @AfterEach
    public void teardown() {
        wireMock.stop();
    }

    public AuraClient buildTestAuraClient(boolean consentConfirmed) {

        AuraClient.AuraClientBuilder builder = new AuraClient.AuraClientBuilder(ctx);
        AuraConsole testConsole = new AuraConsole(TEST_CONSOLE_URL, "deadbeef");
        return builder.withAuraConsole(testConsole)
                .withConsent(consentConfirmed)
                .withUserName("username")
                .withPassword("password".toCharArray())
                .withBoltURI("bolt://hello.com")
                .withClock(org.neo4j.time.Clocks.nanoClock())
                .withCommandResponseHandler(new CommandResponseHandler(ctx))
                .withProgressListenerFactory((name, length) -> new ExportTestUtilities.ControlledProgressListener())
                .withSleeper(millis -> {})
                .build();
    }

    public AuraClient buildTestAuraClientWithMockSleeper(boolean consentConfirmed) {
        var sleeper = mock(IOCommon.Sleeper.class);
        AuraClient.AuraClientBuilder builder = new AuraClient.AuraClientBuilder(ctx);
        AuraConsole testConsole = new AuraConsole(TEST_CONSOLE_URL, "deadbeef");
        return builder.withAuraConsole(testConsole)
                .withConsent(consentConfirmed)
                .withUserName("username")
                .withPassword("password".toCharArray())
                .withBoltURI("bolt://hello.com")
                .withClock(org.neo4j.time.Clocks.nanoClock())
                .withCommandResponseHandler(new CommandResponseHandler(ctx))
                .withProgressListenerFactory(NO_OP_PROGRESS)
                .withSleeper(sleeper)
                .build();
    }

    public AuraClient buildTestAuraClientWithProgressListenerFactory(
            boolean consentConfirmed, AuraClient.ProgressListenerFactory progressListenerFactory) {
        AuraClient.AuraClientBuilder builder = new AuraClient.AuraClientBuilder(ctx);
        AuraConsole testConsole = new AuraConsole(TEST_CONSOLE_URL, "deadbeef");
        return builder.withAuraConsole(testConsole)
                .withConsent(consentConfirmed)
                .withUserName("username")
                .withPassword("password".toCharArray())
                .withBoltURI("bolt://hello.com")
                .withClock(org.neo4j.time.Clocks.nanoClock())
                .withCommandResponseHandler(new CommandResponseHandler(ctx))
                .withProgressListenerFactory(progressListenerFactory)
                .withSleeper(millis -> {})
                .build();
    }

    @Test
    public void runHappyPathTest() throws CommandFailedException, IOException, InterruptedException {
        Path source = createDump();
        ExportTestUtilities.ControlledProgressListener progressListener =
                new ExportTestUtilities.ControlledProgressListener();
        AuraClient.ProgressListenerFactory progressListenerFactory = (name, length) -> progressListener;

        AuraClient auraClient = buildTestAuraClientWithProgressListenerFactory(true, progressListenerFactory);

        long sourceLength = fs.getFileSize(source);
        long dbSize = sourceLength * 4;
        long crc32Sum = 12345;

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        wireMock.stubFor(
                authenticationRequest(true).willReturn(successfulAuthorizationResponse(authorizationTokenResponse)));

        wireMock.stubFor(initiateUploadTargetRequest(authorizationTokenResponse)
                .willReturn(successfulInitiateUploadTargetResponse(signedURIPath)));
        wireMock.stubFor(sizeCheckTargetRequest(authorizationTokenResponse, dbSize)
                .willReturn(successfulSizeCheckTargetResponse()));
        wireMock.stubFor(
                triggerImportRequest(authorizationTokenResponse).willReturn(successfulTriggerImportResponse()));
        wireMock.stubFor(completeUploadTargetRequest(authorizationTokenResponse, crc32Sum)
                .willReturn(successfulCompleteUploadTargetResponse()));
        wireMock.stubFor(firstStatusPollingRequest(authorizationTokenResponse));
        wireMock.stubFor(secondStatusPollingRequest(authorizationTokenResponse));

        //
        //        // when
        auraClient.authenticate(true);
        auraClient.checkSize(true, dbSize, authorizationTokenResponse);
        auraClient.initatePresignedUpload(crc32Sum, dbSize, dbSize, authorizationTokenResponse);
        auraClient.doStatusPolling(true, authorizationTokenResponse, dbSize);
        auraClient.triggerGCPImportProtocol(true, source, crc32Sum, authorizationTokenResponse);
        //
        //        // then
        verify(postRequestedFor(urlMatching(".*?/import/auth$")));
        verify(postRequestedFor(urlMatching(".*?/import/size$"))
                .withRequestBody(matchingJsonPath("FullSize", equalTo(String.valueOf(dbSize)))));

        verify(postRequestedFor(urlMatching(".*?/import$"))
                .withRequestBody(matchingJsonPath("FullSize", equalTo(String.valueOf(dbSize)))));

        verify(postRequestedFor(urlMatching(".*?/import/upload-complete$")));

        assertTrue(progressListener.closeCalled);
        assertEquals(100, progressListener.progress);
        assertTrue(fs.fileExists(source));
        progressListener.close();
    }

    @Test
    void shouldHandleBadCredentialsInAuthorizationRequest() {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        wireMock.stubFor(authenticationRequest(true).willReturn(aResponse().withStatus(HTTP_UNAUTHORIZED)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                CoreMatchers.equalTo("Invalid username/password credentials"),
                () -> auraClient.authenticate(true));
    }

    @Test
    void shouldHandleUnknownDbid() {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        wireMock.stubFor(authenticationRequest(true).willReturn(aResponse().withStatus(HTTP_NOT_FOUND)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                containsString("please check your Bolt URI"),
                () -> auraClient.authenticate(true));
    }

    @Test
    void shouldHandleMoveUploadTargetRoute() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        Path source = createDump();

        long crc32Sum = 12345;
        String authorizationTokenResponse = "abc";

        wireMock.stubFor(
                authenticationRequest(false).willReturn(successfulAuthorizationResponse(authorizationTokenResponse)));
        wireMock.stubFor(
                initiateUploadTargetRequest("abc").willReturn(aResponse().withStatus(HTTP_NOT_FOUND)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                containsString("please contact support"),
                () -> auraClient.triggerGCPImportProtocol(true, source, crc32Sum, authorizationTokenResponse));
    }

    @Test
    void shouldGiveResumableErrorWhenIncorrectInitiateResponse() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        Path source = createDump();

        long crc32Sum = 12345;
        String authorizationTokenResponse = "abc";

        wireMock.stubFor(
                authenticationRequest(false).willReturn(successfulAuthorizationResponse(authorizationTokenResponse)));
        wireMock.stubFor(completeUploadTargetRequest("abc", crc32Sum)
                .willReturn(aResponse().withStatus(429)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                containsString("You can re-try using the existing dump by running this command"),
                () -> auraClient.triggerGCPImportProtocol(true, source, crc32Sum, authorizationTokenResponse));
    }

    @Test
    void shouldHandleImportRequestMovedRoute() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        Path source = createDump();

        long crc32Sum = 12345;
        String authorizationTokenResponse = "abc";

        wireMock.stubFor(
                authenticationRequest(false).willReturn(successfulAuthorizationResponse(authorizationTokenResponse)));
        wireMock.stubFor(completeUploadTargetRequest("abc", crc32Sum)
                .willReturn(aResponse().withStatus(HTTP_NOT_FOUND)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                containsString("please contact support"),
                () -> auraClient.triggerGCPImportProtocol(true, source, crc32Sum, authorizationTokenResponse));
    }

    @Test
    void shouldHandleInsufficientSpaceInSizeRequest() {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        String errorBody =
                "{\"Message\":\"Store is too big for this neo4j aura instance.\",\"Reason\":\"ImportExceedsMaxSize\"}";
        ResponseDefinitionBuilder response =
                aResponse().withStatus(HTTP_UNPROCESSABLE_ENTITY).withBody(errorBody);

        wireMock.stubFor(initiateSizeRequest("fakeToken", 100000000).willReturn(response));
        // when/then
        assertThrows(
                CommandFailedException.class,
                containsString("too big"),
                () -> auraClient.checkSize(false, 100000000, "fakeToken"));
    }

    @Test
    void shouldHandleSufficientSpaceInSizeRequest() {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        ResponseDefinitionBuilder response = aResponse().withStatus(HTTP_OK);
        wireMock.stubFor(initiateSizeRequest("fakeToken", 100000000).willReturn(response));
        // when/then
        auraClient.checkSize(false, 100000000, "fakeToken");
        verify(postRequestedFor(urlMatching(".*?/import/size$")));
    }

    @Test
    void shouldHandleInsufficientCredentialsInAuthorizationRequest() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        wireMock.stubFor(authenticationRequest(true).willReturn(aResponse().withStatus(HTTP_FORBIDDEN)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                containsString("administrative access"),
                () -> auraClient.authenticate(false));
    }

    @Test
    void shouldHandleUnexpectedResponseFromAuthorizationRequest() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        wireMock.stubFor(authenticationRequest(true).willReturn(aResponse().withStatus(HTTP_INTERNAL_ERROR)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                allOf(containsString("Unexpected response"), containsString("Authorization")),
                () -> auraClient.authenticate(false));
    }

    @Test
    void shouldHandleUnauthorizedResponseFromInitiateUploadTarget() throws IOException {
        AuraClient auraClient = buildTestAuraClient(true);

        Path source = createDump();

        long sourceLength = fs.getFileSize(source);
        long dbSize = sourceLength * 4;
        long crc32Sum = 12345;

        String token = "abc";
        wireMock.stubFor(authenticationRequest(true).willReturn(successfulAuthorizationResponse(token)));
        wireMock.stubFor(
                initiateUploadTargetRequest(token).willReturn(aResponse().withStatus(HTTP_UNAUTHORIZED)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                containsString("authorization token is invalid"),
                () -> auraClient.initatePresignedUpload(crc32Sum, dbSize, sourceLength, token));
    }

    @Test
    void shouldHandleValidationFailureResponseFromInitiateUploadTarget() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(true);
        ObjectMapper mapper = new ObjectMapper();
        Path source = createDump();

        long sourceLength = fs.getFileSize(source);
        long dbSize = sourceLength * 4;
        long crc32Sum = 12345;

        String token = "abc";
        String errorMessage = "Dump file rejected for some reason.";
        String errorReason = "some-kind-of-error-reason-code-goes-here";
        String errorUrl = "https://example.com/heres-how-to-fix-this-error";
        AuraJsonMapper.ErrorBody errorBody = new AuraJsonMapper.ErrorBody(errorMessage, errorReason, errorUrl);
        wireMock.stubFor(authenticationRequest(false).willReturn(successfulAuthorizationResponse(token)));
        wireMock.stubFor(initiateUploadTargetRequest(token)
                .willReturn(aResponse()
                        .withBody(mapper.writeValueAsString(errorBody))
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HTTP_UNPROCESSABLE_ENTITY)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                allOf(
                        containsString(errorMessage),
                        containsString(errorUrl),
                        not(containsString(errorReason)),
                        not(containsString(".."))),
                () -> auraClient.initatePresignedUpload(crc32Sum, dbSize, sourceLength, token));
    }

    @Test
    void shouldHandleValidationFailureResponseWithoutUrlFromInitiateUploadTarget() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(false);
        ObjectMapper mapper = new ObjectMapper();
        Path source = createDump();

        long sourceLength = fs.getFileSize(source);
        long dbSize = sourceLength * 4;
        long crc32Sum = 12345;

        String token = "abc";
        String errorMessage = "Something bad happened, but we don't have a URL to share with more information.";
        String errorReason = "the-bad-thing-happened";
        AuraJsonMapper.ErrorBody errorBody = new AuraJsonMapper.ErrorBody(errorMessage, errorReason, null);
        wireMock.stubFor(authenticationRequest(false).willReturn(successfulAuthorizationResponse(token)));
        wireMock.stubFor(initiateUploadTargetRequest(token)
                .willReturn(aResponse()
                        .withBody(mapper.writeValueAsString(errorBody))
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HTTP_UNPROCESSABLE_ENTITY)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                not(containsString("null")),
                () -> auraClient.initatePresignedUpload(crc32Sum, dbSize, sourceLength, token));
    }

    @Test
    void shouldHandleEmptyValidationFailureResponseFromInitiateUploadTarget() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(false);
        Path source = createDump();

        long sourceLength = fs.getFileSize(source);
        long dbSize = sourceLength * 4;
        long crc32Sum = 12345;

        String token = "abc";
        wireMock.stubFor(
                initiateUploadTargetRequest(token).willReturn(aResponse().withStatus(HTTP_UNPROCESSABLE_ENTITY)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                allOf(
                        containsString("No content to map due to end-of-input"),
                        not(containsString("null")),
                        not(containsString(".."))),
                () -> auraClient.initatePresignedUpload(crc32Sum, dbSize, sourceLength, token));
    }

    @Test
    void shouldHandleValidationFailureResponseWithShortMessageFromInitiateUploadTarget() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(false);
        ObjectMapper mapper = new ObjectMapper();
        Path source = createDump();

        long sourceLength = fs.getFileSize(source);
        long dbSize = sourceLength * 4;
        long crc32Sum = 12345;

        String token = "abc";
        // ...and a short error message with no capitalisation or punctuation
        String errorMessage = "something bad happened";
        String errorUrl = "https://example.com/";
        AuraJsonMapper.ErrorBody errorBody = new AuraJsonMapper.ErrorBody(errorMessage, null, errorUrl);
        // ...and
        wireMock.stubFor(initiateUploadTargetRequest(token)
                .willReturn(aResponse()
                        .withBody(mapper.writeValueAsString(errorBody))
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HTTP_UNPROCESSABLE_ENTITY)));

        // when/then the final error message is well formatted with punctuation
        assertThrows(
                CommandFailedException.class,
                containsString("Error: something bad happened. See: https://example.com/"),
                () -> auraClient.initatePresignedUpload(crc32Sum, dbSize, sourceLength, token));
    }

    @Test
    void shouldHandleSizeValidationFailureResponseFromInitiateUploadTarget() throws IOException {
        // given
        AuraClient auraClient = buildTestAuraClient(false);
        ObjectMapper mapper = new ObjectMapper();
        Path source = createDump();

        long sourceLength = fs.getFileSize(source);
        long dbSize = sourceLength * 4;
        long crc32Sum = 12345;

        String token = "abc";
        String errorMessage = "There is insufficient space in your Neo4j Aura instance to upload your data. "
                + "Please use the Console to increase the size of your database.";
        String errorUrl = "https://console.neo4j.io/";
        AuraJsonMapper.ErrorBody errorBody =
                new AuraJsonMapper.ErrorBody(errorMessage, ERROR_REASON_EXCEEDS_MAX_SIZE, errorUrl);
        wireMock.stubFor(authenticationRequest(false).willReturn(successfulAuthorizationResponse(token)));
        wireMock.stubFor(initiateUploadTargetRequest(token)
                .willReturn(aResponse()
                        .withBody(mapper.writeValueAsString(errorBody))
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HTTP_UNPROCESSABLE_ENTITY)));

        // when/then
        assertThrows(
                CommandFailedException.class,
                allOf(
                        containsString(errorMessage),
                        containsString("Minimum storage space required: 0"),
                        containsString("See: https://console.neo4j.io"),
                        not(containsString(".."))),
                () -> auraClient.initatePresignedUpload(crc32Sum, dbSize, sourceLength, token));
    }

    @Test
    void shouldHandleConflictResponseFromAuthenticationWithoutUserConsent() throws IOException {
        AuraClient auraClient = buildTestAuraClient(false);

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        wireMock.stubFor(authenticationRequest(false).willReturn(aResponse().withStatus(HTTP_CONFLICT)));
        wireMock.stubFor(
                authenticationRequest(true).willReturn(successfulAuthorizationResponse(authorizationTokenResponse)));
        wireMock.stubFor(initiateUploadTargetRequest(authorizationTokenResponse)
                .willReturn(successfulInitiateUploadTargetResponse(signedURIPath)));

        // when
        assertThrows(
                CommandFailedException.class,
                containsString("No consent to overwrite"),
                () -> auraClient.authenticate(true));

        // then there should be one request w/o the user consent and then (since the user entered 'y') one w/ user
        // consent
        verify(postRequestedFor(urlMatching(".*?/import/auth$")).withHeader("Confirmed", equalTo("false")));
        verify(0, postRequestedFor(urlMatching(".*?/import/auth$")).withHeader("Confirmed", equalTo("true")));
    }

    @Test
    void shouldHandleUnexpectedResponseFromInitiateUploadTargetRequest() throws IOException {
        AuraClient auraClient = buildTestAuraClient(false);
        Path source = createDump();

        long sourceLength = fs.getFileSize(source);
        long dbSize = sourceLength * 4;
        long crc32Sum = 12345;

        String authorizationTokenResponse = "abc";
        wireMock.stubFor(initiateUploadTargetRequest(authorizationTokenResponse)
                .willReturn(aResponse().withStatus(HTTP_BAD_GATEWAY)));
        // when
        assertThrows(
                CommandFailedException.class,
                allOf(containsString("Unexpected response"), containsString("Initiating upload target")),
                () -> auraClient.initatePresignedUpload(crc32Sum, dbSize, sourceLength, authorizationTokenResponse));

        // 1 initial call plus 2 retries are 3 expected calls
        wireMock.verify(
                51,
                new RequestPatternBuilder(
                        RequestMethod.ANY, UrlPattern.fromOneOf("/v2/databases/deadbeef/import", null, null, null)));
    }

    @Test
    void shouldEstimateImportProgressBased() throws CommandFailedException {
        // given
        AuraClient auraClient = buildTestAuraClientWithMockSleeper(false);

        // when/then
        assertEquals(0, auraClient.importStatusProgressEstimate("running", 1234500000L, 6789000000L));
        assertEquals(1, auraClient.importStatusProgressEstimate("loading", 0, 1234567890));
        // ...and when/then
        assertEquals(2, auraClient.importStatusProgressEstimate("loading", 1, 98));
        assertEquals(50, auraClient.importStatusProgressEstimate("loading", 49, 98));
        assertEquals(98, auraClient.importStatusProgressEstimate("loading", 97, 98));
        assertEquals(99, auraClient.importStatusProgressEstimate("loading", 98, 98));
        assertEquals(99, auraClient.importStatusProgressEstimate("loading", 99, 98));
        assertEquals(99, auraClient.importStatusProgressEstimate("loading", 100, 98));
        // ...and when/then
        assertEquals(1, auraClient.importStatusProgressEstimate("loading", 1, 196));
        assertEquals(2, auraClient.importStatusProgressEstimate("loading", 2, 196));
        assertEquals(50, auraClient.importStatusProgressEstimate("loading", 98, 196));
    }

    private MappingBuilder initiateUploadTargetRequest(String authorizationTokenResponse) {
        return post(urlMatching(".*?/import$"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .withHeader("Accept", equalTo("application/json"))
                // We can't actually test the value that gets set in the header
                // because it comes from the jar's manifest which is not available at test time
                .withHeader("Neo4j-Version", matching(".*"));
    }

    private MappingBuilder sizeCheckTargetRequest(String authorizationTokenResponse, long dbSize) {
        return post(urlMatching(".*?/import/size$"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .withRequestBody(equalToJson("{\"FullSize\": " + dbSize + "}"));
    }

    private MappingBuilder completeUploadTargetRequest(String authorizationTokenResponse, long crc32) {
        return post(urlMatching(".*?/import/upload-complete$"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .withRequestBody(equalToJson("{\"Crc32\":" + crc32 + "}"));
    }

    private ResponseDefinitionBuilder successfulInitiateUploadTargetResponse(String signedURIPath) {
        return aResponse()
                .withStatus(HTTP_ACCEPTED)
                .withBody(format(
                        "{\"SignedURI\":\"%s\", \"expiration_date\":\"Fri, 04 Oct 2019 08:21:59 GMT\"}",
                        TEST_CONSOLE_URL + signedURIPath));
    }

    private ResponseDefinitionBuilder successfulSizeCheckTargetResponse() {
        return aResponse().withStatus(HTTP_OK);
    }

    private MappingBuilder initiateSizeRequest(String authorizationTokenResponse, long size) {
        return post(urlMatching(".*?/import/size$"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .withHeader("Content-Type", equalTo("application/json"));
    }

    private Path createDump() throws IOException {
        final var file = directory.file("something");
        try (var outputStream = fs.openAsOutputStream(file, false);
                var out = new PrintStream(outputStream)) {
            out.println("this is simply some weird dump data, but may do the trick for this test of uploading it");
        }
        return file;
    }

    private MappingBuilder authenticationRequest(boolean userConsent) {
        return post(urlMatching(".*?/import/auth$"))
                .withHeader("Authorization", matching("^Basic .*"))
                .withHeader("Accept", equalTo("application/json"))
                .withHeader("Confirmed", equalTo(userConsent ? "true" : "false"));
    }

    private ResponseDefinitionBuilder successfulAuthorizationResponse(String authorizationTokenResponse) {
        return aResponse().withStatus(HTTP_OK).withBody(format("{\"Token\":\"%s\"}", authorizationTokenResponse));
    }

    private MappingBuilder triggerImportRequest(String authorizationTokenResponse) {
        return post(urlMatching(".*?/import/upload-complete$"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .withRequestBody(containing("Crc32"));
    }

    private ResponseDefinitionBuilder successfulTriggerImportResponse() {
        return aResponse().withStatus(HTTP_OK);
    }

    private ResponseDefinitionBuilder successfulCompleteUploadTargetResponse() {
        return aResponse().withStatus(HTTP_OK);
    }

    private MappingBuilder firstStatusPollingRequest(String authorizationTokenResponse) {
        return get(urlMatching(".*?/import/status$"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(firstSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(STATUS_POLLING_PASSED_SECOND_CALL);
    }

    private MappingBuilder secondStatusPollingRequest(String authorizationTokenResponse) {
        return get(urlMatching(".*?/import/status$"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(secondSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_SECOND_CALL);
    }

    private ResponseDefinitionBuilder firstSuccessfulDatabaseRunningResponse() {
        return aResponse().withBody("{\"Status\":\"loading\"}").withStatus(HTTP_OK);
    }

    private ResponseDefinitionBuilder secondSuccessfulDatabaseRunningResponse() {
        return aResponse().withBody("{\"Status\":\"running\"}").withStatus(HTTP_OK);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
