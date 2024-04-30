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
package org.neo4j.pushtocloud;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.pushtocloud.HttpCopier.ErrorBody;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.SystemNanoClock;
import wiremock.com.fasterxml.jackson.databind.ObjectMapper;
import wiremock.org.hamcrest.CoreMatchers;
import wiremock.org.hamcrest.Matcher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;
import static org.neo4j.pushtocloud.HttpCopier.ERROR_REASON_EXCEEDS_MAX_SIZE;
import static org.neo4j.pushtocloud.HttpCopier.ERROR_REASON_UNSUPPORTED_INDEXES;
import static org.neo4j.pushtocloud.HttpCopier.HTTP_RESUME_INCOMPLETE;
import static org.neo4j.pushtocloud.HttpCopier.HTTP_UNPROCESSABLE_ENTITY;
import static org.neo4j.pushtocloud.HttpCopier.StatusBody;
import static wiremock.org.hamcrest.CoreMatchers.allOf;
import static wiremock.org.hamcrest.CoreMatchers.containsString;
import static wiremock.org.hamcrest.MatcherAssert.assertThat;
import static wiremock.org.hamcrest.Matchers.not;

@TestDirectoryExtension
class HttpCopierTest
{
    private static final HttpCopier.ProgressListenerFactory NO_OP_PROGRESS = ( name, length ) -> ProgressListener.NONE;

    private static final int TEST_PORT = 8080;
    private static final String TEST_CONSOLE_URL = "http://localhost:" + TEST_PORT;
    private static final String STATUS_POLLING_PASSED_FIRST_CALL = "Passed first";
    private static final String STATUS_POLLING_PASSED_SECOND_CALL = "Passed second";

    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    WireMockServer wireMock;
    @Inject
    TestDirectory directory;
    private ExecutionContext ctx;

    private static void assertThrows( Class<? extends Exception> exceptionClass, Matcher<String> message, ThrowingRunnable action )
    {
        try
        {
            action.run();
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertTrue( exceptionClass.isInstance( e ) );
            assertThat( e.getMessage(), message );
        }
    }

    @BeforeEach
    public void setup()
    {
        wireMock = new WireMockServer( TEST_PORT );
        wireMock.start();
        Path dir = directory.homePath();
        PrintStream out = new PrintStream( NULL_OUTPUT_STREAM );
        ctx = new ExecutionContext( dir, dir, out, out, fs );
    }

    @AfterEach
    public void teardown()
    {
        wireMock.stop();
    }

    @Test
    void shouldHandleSuccessfulHappyCaseRunThroughOfTheWholeProcess() throws Exception
    {
        // create dump
        Path source = createDump();
        runHappyPathTest( source, true );
        // assert dump was deleted
        assertFalse( Files.exists( source ) );
    }

    @Test
    void shouldHandleSuccessfulHappyCaseRunThroughOfTheWholeProcessWithExistingDump() throws Exception
    {
        // create dump
        Path source = createDump();
        runHappyPathTest( source, false );
        // assert externally provided dump was not deleted
        assertTrue( Files.exists( source ) );
    }

    private void runHappyPathTest( Path source, boolean sourceProvided ) throws CommandFailedException, IOException
    {
        // given
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( ctx, millis ->
        {
        }, ( name, length ) -> progressListener );

        long sourceLength = fs.getFileSize( source );
        long dbSize = sourceLength * 4;

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength ).willReturn( successfulResumeUploadResponse() ) );
        wireMock.stubFor( triggerImportRequest( authorizationTokenResponse ).willReturn( successfulTriggerImportResponse() ) );
        wireMock.stubFor( firstStatusPollingRequest( authorizationTokenResponse ) );
        wireMock.stubFor( secondStatusPollingRequest( authorizationTokenResponse ) );

        // when
        authenticateAndCopy( copier, source, dbSize, sourceProvided, "user", "pass".toCharArray() );

        // then
        verify( postRequestedFor( urlEqualTo( "/import/auth" ) ) );
        verify( postRequestedFor( urlEqualTo( "/import" ) )
                        .withRequestBody( matchingJsonPath( "FullSize", equalTo( String.valueOf( dbSize ) ) ) ) );
        verify( postRequestedFor( urlEqualTo( signedURIPath ) ) );
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) ) );
        verify( postRequestedFor( urlEqualTo( "/import/upload-complete" ) ) );
        assertTrue( progressListener.doneCalled );
        // we need to add 100 extra ticks to the progress listener because of the database phases
        assertEquals( sourceLength + 100, progressListener.progress );
    }

    @Test
    void shouldHandleResumableFailureWhileUploading() throws Exception
    {
        // given
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( ctx, millis ->
        {
        }, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength ).willReturn( aResponse().withStatus( HttpCopier.HTTP_TOO_MANY_REQUESTS ) ) );

        // when
        assertThrows( CommandFailedException.class, containsString( "You can re-try using the existing dump by running this command" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleResumableFailureWhenImportIsTriggered() throws Exception
    {
        // given
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( ctx, millis ->
        {
        }, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength ).willReturn( successfulResumeUploadResponse() ) );
        wireMock.stubFor( triggerImportRequest( authorizationTokenResponse ).willReturn( aResponse().withStatus( HttpCopier.HTTP_TOO_MANY_REQUESTS ) ) );
        wireMock.stubFor( firstStatusPollingRequest( authorizationTokenResponse ) );
        wireMock.stubFor( secondStatusPollingRequest( authorizationTokenResponse ) );

        // when
        assertThrows( CommandFailedException.class, containsString( "You can re-try using the existing dump by running this command" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleBadCredentialsInAuthorizationRequest() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                                                                             .withStatus( HTTP_UNAUTHORIZED ) ) );

        // when/then
        assertThrows( CommandFailedException.class, CoreMatchers.equalTo( "Invalid username/password credentials" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleUnknownDbid() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                                                                             .withStatus( HTTP_NOT_FOUND ) ) );

        // when/then
        assertThrows( CommandFailedException.class, containsString( "please check your Bolt URI" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleMoveUploadTargetRoute() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";

        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( "abc" ).willReturn( aResponse()
                                                                                   .withStatus( HTTP_NOT_FOUND ) ) );

        // when/then
        assertThrows( CommandFailedException.class, containsString( "please contact support" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleImportRequestMovedRoute() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";

        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength ).willReturn( successfulResumeUploadResponse() ) );

        wireMock.stubFor( triggerImportRequest( "abc" ).willReturn( aResponse()
                                                                            .withStatus( HTTP_NOT_FOUND ) ) );

        // when/then
        assertThrows( CommandFailedException.class, containsString( "please contact support" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleInsufficientSpaceInSizeRequest()
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        String errorBody = format( "{\"Message\":\"Store is too big for this neo4j aura instance.\",\"Reason\":\"ImportExceedsMaxSize\"}" );
        ResponseDefinitionBuilder response = aResponse().withStatus( HTTP_UNPROCESSABLE_ENTITY ).withBody( errorBody );
        wireMock.stubFor( initiateSizeRequest( "fakeToken", 100000000 ).willReturn( response ) );
        // when/then
        assertThrows( CommandFailedException.class, containsString( "too big" ),
                () -> copier.checkSize( false, TEST_CONSOLE_URL, 100000000, "fakeToken" ) );
    }

    @Test
    void shouldHandleSufficientSpaceInSizeRequest()
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        ResponseDefinitionBuilder response = aResponse().withStatus( HTTP_OK );
        wireMock.stubFor( initiateSizeRequest( "fakeToken", 100000000 ).willReturn( response ) );
        // when/then
        copier.checkSize( false, TEST_CONSOLE_URL, 100000000, "fakeToken" );
        verify( postRequestedFor( urlEqualTo( "/import/size" ) ) );
    }

    @Test
    void shouldHandleInsufficientCredentialsInAuthorizationRequest() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                                                                             .withStatus( HTTP_FORBIDDEN ) ) );

        // when/then
        assertThrows( CommandFailedException.class, containsString( "administrative access" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleUnexpectedResponseFromAuthorizationRequest() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                                                                             .withStatus( HTTP_INTERNAL_ERROR ) ) );

        // when/then
        assertThrows( CommandFailedException.class, allOf( containsString( "Unexpected response" ), containsString( "Authorization" ) ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleUnauthorizedResponseFromInitiateUploadTarget() throws IOException
    {
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        String token = "abc";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( token ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( token ).willReturn( aResponse()
                                                                                   .withStatus( HTTP_UNAUTHORIZED ) ) );

        // when/then
        assertThrows( CommandFailedException.class, containsString( "authorization token is invalid" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleValidationFailureResponseFromInitiateUploadTarget() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        ObjectMapper mapper = new ObjectMapper();
        Path source = createDump();
        String token = "abc";
        String errorMessage = "Dump file rejected for some reason.";
        String errorReason = "some-kind-of-error-reason-code-goes-here";
        String errorUrl = "https://example.com/heres-how-to-fix-this-error";
        ErrorBody errorBody = new ErrorBody( errorMessage, errorReason, errorUrl );
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( token ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( token )
                                  .willReturn( aResponse()
                                                       .withBody( mapper.writeValueAsString( errorBody ) )
                                                       .withHeader( "Content-Type", "application/json" )
                                                       .withStatus( HTTP_UNPROCESSABLE_ENTITY ) ) );

        // when/then
        assertThrows( CommandFailedException.class,
                      allOf( containsString( errorMessage ), containsString( errorUrl ), not( containsString( errorReason ) ),
                             not( containsString( ".." ) ) ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleValidationFailureResponseWithoutUrlFromInitiateUploadTarget() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        ObjectMapper mapper = new ObjectMapper();
        Path source = createDump();
        String token = "abc";
        String errorMessage = "Something bad happened, but we don't have a URL to share with more information.";
        String errorReason = "the-bad-thing-happened";
        ErrorBody errorBody = new ErrorBody( errorMessage, errorReason, null );
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( token ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( token )
                                  .willReturn( aResponse()
                                                       .withBody( mapper.writeValueAsString( errorBody ) )
                                                       .withHeader( "Content-Type", "application/json" )
                                                       .withStatus( HTTP_UNPROCESSABLE_ENTITY ) ) );

        // when/then
        assertThrows( CommandFailedException.class, not( containsString( "null" ) ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleEmptyValidationFailureResponseFromInitiateUploadTarget() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        String token = "abc";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( token ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( token )
                                  .willReturn( aResponse().withStatus( HTTP_UNPROCESSABLE_ENTITY ) ) );

        // when/then
        assertThrows( CommandFailedException.class,
                      allOf( containsString( "No content to map due to end-of-input" ),
                             not( containsString( "null" ) ),
                             not( containsString( ".." ) ) ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleValidationFailureResponseWithShortMessageFromInitiateUploadTarget() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        ObjectMapper mapper = new ObjectMapper();
        Path source = createDump();
        String token = "abc";
        // ...and a short error message with no capitalisation or punctuation
        String errorMessage = "something bad happened";
        String errorUrl = "https://example.com/";
        ErrorBody errorBody = new ErrorBody( errorMessage, null, errorUrl );
        // ...and
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( token ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( token )
                                  .willReturn( aResponse().withBody( mapper.writeValueAsString( errorBody ) )
                                                          .withHeader( "Content-Type", "application/json" ).withStatus( HTTP_UNPROCESSABLE_ENTITY ) ) );

        // when/then the final error message is well formatted with punctuation
        assertThrows( CommandFailedException.class, containsString( "Error: something bad happened. See: https://example.com/" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleSizeValidationFailureResponseFromInitiateUploadTarget() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        ObjectMapper mapper = new ObjectMapper();
        Path source = createDump();
        String token = "abc";
        String errorMessage = "There is insufficient space in your Neo4j Aura instance to upload your data. "
                              + "Please use the Console to increase the size of your database.";
        String errorUrl = "https://console.neo4j.io/";
        ErrorBody errorBody = new ErrorBody( errorMessage, ERROR_REASON_EXCEEDS_MAX_SIZE, errorUrl );
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( token ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( token )
                                  .willReturn( aResponse().withBody( mapper.writeValueAsString( errorBody ) )
                                                          .withHeader( "Content-Type", "application/json" ).withStatus( HTTP_UNPROCESSABLE_ENTITY ) ) );

        // when/then
        assertThrows( CommandFailedException.class,
                      allOf( containsString( errorMessage ),
                             containsString( "Minimum storage space required: 0" ),
                             containsString( "See: https://console.neo4j.io" ), not( containsString( ".." ) ) ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleConflictResponseFromAuthenticationWithoutUserConsent() throws IOException
    {
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse().withStatus( HTTP_CONFLICT ) ) );
        wireMock.stubFor( authenticationRequest( true ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );

        // when
        assertThrows( CommandFailedException.class, containsString( "No consent to overwrite" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );

        // then there should be one request w/o the user consent and then (since the user entered 'y') one w/ user consent
        verify( postRequestedFor( urlEqualTo( "/import/auth" ) ).withHeader( "Confirmed", equalTo( "false" ) ) );
        verify( 0, postRequestedFor( urlEqualTo( "/import/auth" ) ).withHeader( "Confirmed", equalTo( "true" ) ) );
    }

    @Test
    void shouldHandleUnexpectedResponseFromInitiateUploadTargetRequest() throws IOException
    {
        HttpCopier copier = new HttpCopier( ctx, 2, 2 );
        Path source = createDump();
        String authorizationTokenResponse = "abc";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse ).willReturn( aResponse()
                                                                                                        .withStatus( HTTP_BAD_GATEWAY ) ) );
        // when
        assertThrows( CommandFailedException.class, allOf( containsString( "Unexpected response" ), containsString( "Initiating upload target" ) ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
        // 1 initial call plus 2 retries are 3 expected calls
        wireMock.verify(3, new RequestPatternBuilder( RequestMethod.ANY, UrlPattern.fromOneOf( "/import", null, null, null ) ) );

        // increase maximum retries
        HttpCopier copier2 = new HttpCopier( ctx, 20, 2 );
        // and call it again.
        assertThrows( CommandFailedException.class, allOf( containsString( "Unexpected response" ), containsString( "Initiating upload target" ) ),
                () -> authenticateAndCopy( copier2, source, 1234, true, "user", "pass".toCharArray() ) );
        // 24 = 3 from previous copier + 1 initial call + 20 retries
        wireMock.verify(24, new RequestPatternBuilder( RequestMethod.ANY, UrlPattern.fromOneOf( "/import", null, null, null ) ) );
    }

    @Test
    void shouldHandleInitiateUploadFailure() throws IOException
    {
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( aResponse()
                                                                                     .withStatus( HTTP_INTERNAL_ERROR ) ) );

        // when
        assertThrows( CommandFailedException.class, allOf( containsString( "Unexpected response" ), containsString( "Initiating database upload" ) ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleUploadStartTimeoutFailure() throws IOException
    {
        // given
        ObjectMapper mapper = new ObjectMapper();
        ControlledProgressListener progressListener = new ControlledProgressListener();
        SystemNanoClock clockMock = mock( SystemNanoClock.class );
        HttpCopier copier = new HttpCopier( ctx, millis -> {}, ( name, length ) -> progressListener, clockMock, 10, 10 );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );
        long dbSize = sourceLength * 4;
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";

        wireMock.stubFor(
                authenticationRequest(false).willReturn(successfulAuthorizationResponse(authorizationTokenResponse)));
        wireMock.stubFor(initiateUploadTargetRequest(authorizationTokenResponse)
                .willReturn(successfulInitiateUploadTargetResponse(signedURIPath)));
        wireMock.stubFor(
                initiateUploadRequest(signedURIPath).willReturn(successfulInitiateUploadResponse(uploadLocationPath)));
        wireMock.stubFor(
                resumeUploadRequest(uploadLocationPath, sourceLength).willReturn(successfulResumeUploadResponse()));
        wireMock.stubFor(
                triggerImportRequest(authorizationTokenResponse).willReturn(successfulTriggerImportResponse()));
        // ...and

        // the database has status "running"
        wireMock.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(secondSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(STATUS_POLLING_PASSED_FIRST_CALL));

        // the load should have started but database still has status "running"
        wireMock.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(secondSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_FIRST_CALL));

        when(clockMock.millis()).thenReturn(0L).thenReturn(120 * 1000L);

        // when
        assertThrows(
                CommandFailedException.class,
                containsString("Timed out waiting for database load to start"),
                () -> authenticateAndCopy(copier, source, dbSize, true, "user", "pass".toCharArray()));
    }

    @Test
    void shouldHandleUploadInACoupleOfRounds() throws IOException, CommandFailedException
    {
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( ctx, millis ->
        {
        }, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );
        long firstUploadLength = sourceLength / 3;
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, 0, sourceLength ).willReturn( aResponse()
                                                                                                         .withStatus( HTTP_INTERNAL_ERROR ) ) );
        wireMock.stubFor( getResumablePositionRequest( sourceLength, uploadLocationPath )
                                  .willReturn( uploadIncompleteGetResumablePositionResponse( firstUploadLength ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, firstUploadLength, sourceLength )
                                  .willReturn( successfulResumeUploadResponse() ) );
        wireMock.stubFor( triggerImportRequest( authorizationTokenResponse ).willReturn( successfulTriggerImportResponse() ) );
        wireMock.stubFor( firstStatusPollingRequest( authorizationTokenResponse ) );
        wireMock.stubFor( secondStatusPollingRequest( authorizationTokenResponse ) );

        // when
        authenticateAndCopy( copier, source, sourceLength * 4, true, "user", "pass".toCharArray() );

        // then
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) )
                        .withHeader( "Content-Length", equalTo( Long.toString( sourceLength ) ) )
                        .withoutHeader( "Content-Range" ) );
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) )
                        .withHeader( "Content-Length", equalTo( Long.toString( sourceLength - firstUploadLength ) ) )
                        .withHeader( "Content-Range", equalTo( format( "bytes %d-%d/%d", firstUploadLength, sourceLength - 1, sourceLength ) ) ) );
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) )
                        .withHeader( "Content-Length", equalTo( "0" ) )
                        .withHeader( "Content-Range", equalTo( "bytes */" + sourceLength ) ) );
        assertTrue( progressListener.doneCalled );
        // we need to add 100 extra ticks to the progress listener because of the database phases
        assertEquals( sourceLength + 100, progressListener.progress );
    }

    @Test
    void shouldHandleFailedImport() throws IOException, CommandFailedException
    {
        // given
        ObjectMapper mapper = new ObjectMapper();
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( ctx, millis ->
        {
        }, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";

        wireMock.stubFor( authenticationRequest( false )
                                  .willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath )
                                  .willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength )
                                  .willReturn( successfulResumeUploadResponse() ) );
        wireMock.stubFor(
                triggerImportRequest( authorizationTokenResponse ).willReturn( successfulTriggerImportResponse() ) );
        wireMock.stubFor(firstStatusPollingRequest(authorizationTokenResponse));
        // ...and
        StatusBody statusBody = new StatusBody();
        statusBody.Status = "loading failed";
        String errorMessage = "The uploaded dump file contains deprecated indexes, "
                              + "which we are unable to import in the current version of Neo4j Aura. "
                              + "Please upgrade to the recommended index provider.";
        String errorUrl = "https://support.neo4j.com";
        statusBody.Error = new ErrorBody( errorMessage, ERROR_REASON_UNSUPPORTED_INDEXES, errorUrl );

        wireMock.stubFor( get( urlEqualTo( "/import/status" ) )
                                  .withHeader( "Authorization", equalTo( "Bearer " + authorizationTokenResponse ) )
                                  .willReturn( aResponse().withBody( mapper.writeValueAsString( statusBody ) )
                                                          .withHeader( "Content-Type", "application/json" ).withStatus( HTTP_OK ) )
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_FIRST_CALL));

        // when/then
        assertThrows( CommandFailedException.class,
                      allOf( containsString( errorMessage ), containsString( errorUrl ),
                             not( containsString( ERROR_REASON_UNSUPPORTED_INDEXES ) ), not( containsString( ".." ) ) ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldHandleFailedImportStatusFromPreviousLoad() throws IOException, CommandFailedException
    {
        // given
        ObjectMapper mapper = new ObjectMapper();
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( ctx, millis -> {}, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );
        long dbSize = sourceLength * 4;
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";

        wireMock.stubFor(
                authenticationRequest(false).willReturn(successfulAuthorizationResponse(authorizationTokenResponse)));
        wireMock.stubFor(initiateUploadTargetRequest(authorizationTokenResponse)
                .willReturn(successfulInitiateUploadTargetResponse(signedURIPath)));
        wireMock.stubFor(
                initiateUploadRequest(signedURIPath).willReturn(successfulInitiateUploadResponse(uploadLocationPath)));
        wireMock.stubFor(
                resumeUploadRequest(uploadLocationPath, sourceLength).willReturn(successfulResumeUploadResponse()));
        wireMock.stubFor(
                triggerImportRequest(authorizationTokenResponse).willReturn(successfulTriggerImportResponse()));
        // ...and
        StatusBody statusBody = new StatusBody();
        statusBody.Status = "loading failed";
        String errorMessage = "The uploaded dump file contains deprecated indexes, "
                + "which we are unable to import in the current version of Neo4j Aura. "
                + "Please upgrade to the recommended index provider.";
        String errorUrl = "https://support.neo4j.com";
        statusBody.Error = new ErrorBody( errorMessage, ERROR_REASON_UNSUPPORTED_INDEXES, errorUrl );

        wireMock.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(aResponse()
                        .withBody(mapper.writeValueAsString(statusBody))
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HTTP_OK))
                .inScenario("test")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo(STATUS_POLLING_PASSED_FIRST_CALL));

        wireMock.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(firstSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_FIRST_CALL)
                .willSetStateTo(STATUS_POLLING_PASSED_SECOND_CALL));

        wireMock.stubFor(get(urlEqualTo("/import/status"))
                .withHeader("Authorization", equalTo("Bearer " + authorizationTokenResponse))
                .willReturn(secondSuccessfulDatabaseRunningResponse())
                .inScenario("test")
                .whenScenarioStateIs(STATUS_POLLING_PASSED_SECOND_CALL));

        // when
        authenticateAndCopy(copier, source, dbSize, true, "user", "pass".toCharArray());

        // then
        verify(postRequestedFor(urlEqualTo("/import/auth")));
        verify(postRequestedFor(urlEqualTo("/import"))
                .withRequestBody(matchingJsonPath("FullSize", equalTo(String.valueOf(dbSize)))));
        verify(postRequestedFor(urlEqualTo(signedURIPath)));
        verify(putRequestedFor(urlEqualTo(uploadLocationPath)));
        verify(postRequestedFor(urlEqualTo("/import/upload-complete")));
        assertTrue(progressListener.doneCalled);
        // we need to add 100 extra ticks to the progress listener because of the database phases
        assertEquals(sourceLength + 100, progressListener.progress);
    }

    @Test
    void shouldHandleIncompleteUploadButPositionSaysComplete() throws IOException, CommandFailedException
    {
        HttpCopier copier = new HttpCopier( ctx, millis ->
        {
        }, NO_OP_PROGRESS );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, 0, sourceLength ).willReturn( aResponse()
                                                                                                         .withStatus( HTTP_INTERNAL_ERROR ) ) );
        wireMock.stubFor( getResumablePositionRequest( sourceLength, uploadLocationPath )
                                  .willReturn( uploadCompleteGetResumablePositionResponse() ) );
        wireMock.stubFor( triggerImportRequest( authorizationTokenResponse ).willReturn( successfulTriggerImportResponse() ) );
        wireMock.stubFor( firstStatusPollingRequest( authorizationTokenResponse ) );
        wireMock.stubFor( secondStatusPollingRequest( authorizationTokenResponse ) );

        // when
        authenticateAndCopy( copier, source, sourceLength * 4, true, "user", "pass".toCharArray() );

        // then
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) )
                        .withHeader( "Content-Length", equalTo( Long.toString( sourceLength ) ) )
                        .withoutHeader( "Content-Range" ) );
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) )
                        .withHeader( "Content-Length", equalTo( "0" ) )
                        .withHeader( "Content-Range", equalTo( "bytes */" + sourceLength ) ) );
    }

    @Test
    void shouldHandleConflictOnTriggerImportAfterUpload() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( ctx );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength ).willReturn( successfulResumeUploadResponse() ) );
        wireMock.stubFor( triggerImportRequest( authorizationTokenResponse ).willReturn( aResponse()
                                                                                                 .withStatus( HTTP_CONFLICT ) ) );

        // when
        assertThrows( CommandFailedException.class, containsString( "The target database contained data and consent to overwrite the data was not given." ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
    }

    @Test
    void shouldBackoffAndFailIfTooManyAttempts() throws IOException, InterruptedException
    {
        // given
        HttpCopier.Sleeper sleeper = mock( HttpCopier.Sleeper.class );
        HttpCopier copier = new HttpCopier( ctx, sleeper, NO_OP_PROGRESS );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source );
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                                  .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength ).willReturn( aResponse()
                                                                                                      .withStatus( HTTP_INTERNAL_ERROR ) ) );
        wireMock.stubFor( getResumablePositionRequest( sourceLength, uploadLocationPath )
                                  .willReturn( uploadIncompleteGetResumablePositionResponse( 0 ) ) );

        // when/then
        assertThrows( CommandFailedException.class, containsString( "Upload failed after numerous attempts" ),
                      () -> authenticateAndCopy( copier, source, 1234, true, "user", "pass".toCharArray() ) );
        Mockito.verify( sleeper, atLeast( 30 ) ).sleep( anyLong() );
    }

    @Test
    void shouldSkipToImport() throws IOException
    {
        HttpCopier.Sleeper sleeper = mock( HttpCopier.Sleeper.class );
        HttpCopier copier = new HttpCopier( ctx, sleeper, NO_OP_PROGRESS );
        String error = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>AccessDenied</Code><Message>Access denied." +
                "</Message><Details>hello@hello.iam.gserviceaccount.com does not have storage.objects.delete " +
                "access to the Google Cloud Storage object.</Details></Error>";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( error.getBytes() );
        assertTrue( copier.canSkipToImport( byteArrayInputStream ) );
    }

    @Test
    void shouldNotSkipToImport() throws IOException
    {
        HttpCopier.Sleeper sleeper = mock( HttpCopier.Sleeper.class );
        HttpCopier copier = new HttpCopier( ctx, sleeper, NO_OP_PROGRESS );
        String error = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>AccessDenied</Code><Message>Access denied." +
                "</Message><Details>hello@hello.iam.gserviceaccount.com " +
                " has a problem we haven't thought about</Details></Error>";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( error.getBytes() );
        assertFalse( copier.canSkipToImport( byteArrayInputStream ) );
    }

    @Test
    void shouldThrowErrorParsingXEEVulnerableContent()
    {
        HttpCopier.Sleeper sleeper = mock( HttpCopier.Sleeper.class );
        HttpCopier copier = new HttpCopier( ctx, sleeper, NO_OP_PROGRESS );
        String error = "<?xml version='1.0' encoding='UTF-8'?>" +
                "<!DOCTYPE foo [ <!ENTITY xxe SYSTEM \"file:///etc/ntp.conf\"> ]>" +
                "<Error><Code>&xxe;</Code><Message>Access denied." +
                "</Message><Details>hello@hello.iam.gserviceaccount.com does not have storage.objects.delete " +
                "access to the Google Cloud Storage object.</Details></Error>";
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( error.getBytes() );

        assertThrows( IOException.class, containsString("Encountered invalid response from cloud import location" ),
                () -> copier.canSkipToImport( byteArrayInputStream ) );
    }

    @Test
    void shouldEstimateImportProgressBased() throws CommandFailedException
    {
        // given
        HttpCopier.Sleeper sleeper = mock( HttpCopier.Sleeper.class );
        HttpCopier copier = new HttpCopier( ctx, sleeper, NO_OP_PROGRESS );
        // when/then
        assertEquals( 0, copier.importStatusProgressEstimate( "running", 1234500000L, 6789000000L ) );
        assertEquals( 1, copier.importStatusProgressEstimate( "loading", 0, 1234567890 ) );
        // ...and when/then
        assertEquals( 2, copier.importStatusProgressEstimate( "loading", 1, 98 ) );
        assertEquals( 50, copier.importStatusProgressEstimate( "loading", 49, 98 ) );
        assertEquals( 98, copier.importStatusProgressEstimate( "loading", 97, 98 ) );
        assertEquals( 99, copier.importStatusProgressEstimate( "loading", 98, 98 ) );
        assertEquals( 99, copier.importStatusProgressEstimate( "loading", 99, 98 ) );
        assertEquals( 99, copier.importStatusProgressEstimate( "loading", 100, 98 ) );
        // ...and when/then
        assertEquals( 1, copier.importStatusProgressEstimate( "loading", 1, 196 ) );
        assertEquals( 2, copier.importStatusProgressEstimate( "loading", 2, 196 ) );
        assertEquals( 50, copier.importStatusProgressEstimate( "loading", 98, 196 ) );
    }

    private MappingBuilder authenticationRequest( boolean userConsent )
    {
        return post( urlEqualTo( "/import/auth" ) )
                .withHeader( "Authorization", matching( "^Basic .*" ) )
                .withHeader( "Accept", equalTo( "application/json" ) )
                .withHeader( "Confirmed", equalTo( userConsent ? "true" : "false" ) );
    }

    private ResponseDefinitionBuilder successfulAuthorizationResponse( String authorizationTokenResponse )
    {
        return aResponse()
                .withStatus( HTTP_OK )
                .withBody( format( "{\"Token\":\"%s\"}", authorizationTokenResponse ) );
    }

    private MappingBuilder initiateUploadTargetRequest( String authorizationTokenResponse )
    {
        return post( urlEqualTo( "/import" ) )
                .withHeader( "Content-Type", equalTo( "application/json" ) )
                .withHeader( "Authorization", equalTo( "Bearer " + authorizationTokenResponse ) )
                .withHeader( "Accept", equalTo( "application/json" ) );
    }

    private MappingBuilder initiateSizeRequest( String authorizationTokenResponse, long size )
    {
        return post( urlEqualTo( "/import/size" ) )
                .withHeader( "Authorization", equalTo( "Bearer " + authorizationTokenResponse ) )
                .withHeader( "Content-Type", equalTo( "application/json" ) );
    }

    private ResponseDefinitionBuilder successfulInitiateUploadTargetResponse( String signedURIPath )
    {
        return aResponse()
                .withStatus( HTTP_ACCEPTED )
                .withBody( format( "{\"SignedURI\":\"%s\", \"expiration_date\":\"Fri, 04 Oct 2019 08:21:59 GMT\"}", TEST_CONSOLE_URL + signedURIPath ) );
    }

    private MappingBuilder initiateUploadRequest( String signedURIPath )
    {
        return post( urlEqualTo( signedURIPath ) )
                .withHeader( "Content-Length", equalTo( "0" ) )
                .withHeader( "x-goog-resumable", equalTo( "start" ) );
    }

    private ResponseDefinitionBuilder successfulInitiateUploadResponse( String uploadLocationPath )
    {
        return aResponse()
                .withStatus( HTTP_CREATED )
                .withHeader( "Location", TEST_CONSOLE_URL + uploadLocationPath );
    }

    private MappingBuilder resumeUploadRequest( String uploadLocationPath, long length )
    {
        return resumeUploadRequest( uploadLocationPath, 0, length );
    }

    private MappingBuilder resumeUploadRequest( String uploadLocationPath, long position, long length )
    {
        MappingBuilder builder = put( urlEqualTo( uploadLocationPath ) )
                .withHeader( "Content-Length", equalTo( Long.toString( length - position ) ) );
        if ( position > 0 )
        {
            builder = builder.withHeader( "Content-Range", equalTo( format( "bytes %d-%d/%d", position, length - 1, length ) ) );
        }
        return builder;
    }

    private ResponseDefinitionBuilder successfulResumeUploadResponse()
    {
        return aResponse()
                .withStatus( HTTP_OK );
    }

    private MappingBuilder firstStatusPollingRequest( String authorizationTokenResponse )
    {
        return get( urlEqualTo( "/import/status" ) )
                .withHeader( "Authorization", equalTo( "Bearer " + authorizationTokenResponse ) )
                .willReturn( firstSuccessfulDatabaseRunningResponse() )
                .inScenario( "test" )
                .whenScenarioStateIs( Scenario.STARTED )
                .willSetStateTo( STATUS_POLLING_PASSED_FIRST_CALL );
    }

    private ResponseDefinitionBuilder firstSuccessfulDatabaseRunningResponse()
    {
        return aResponse()
                .withBody( "{\"Status\":\"loading\"}" )
                .withStatus( HTTP_OK );
    }

    private MappingBuilder secondStatusPollingRequest( String authorizationTokenResponse )
    {
        return get( urlEqualTo( "/import/status" ) )
                .withHeader( "Authorization", equalTo( "Bearer " + authorizationTokenResponse ) )
                .willReturn( secondSuccessfulDatabaseRunningResponse() )
                .inScenario( "test" )
                .whenScenarioStateIs( STATUS_POLLING_PASSED_FIRST_CALL );
    }

    private ResponseDefinitionBuilder secondSuccessfulDatabaseRunningResponse()
    {
        return aResponse()
                .withBody( "{\"Status\":\"running\"}" )
                .withStatus( HTTP_OK );
    }

    private MappingBuilder triggerImportRequest( String authorizationTokenResponse )
    {
        return post( urlEqualTo( "/import/upload-complete" ) )
                .withHeader( "Content-Type", equalTo( "application/json" ) )
                .withHeader( "Authorization", equalTo( "Bearer " + authorizationTokenResponse ) )
                .withRequestBody( containing( "Crc32" ) );
    }

    private ResponseDefinitionBuilder successfulTriggerImportResponse()
    {
        return aResponse()
                .withStatus( HTTP_OK );
    }

    private ResponseDefinitionBuilder uploadIncompleteGetResumablePositionResponse( long bytesUploadedSoFar )
    {
        return aResponse()
                .withStatus( HTTP_RESUME_INCOMPLETE )
                .withHeader( "Range", "bytes=0-" + (bytesUploadedSoFar - 1) );
    }

    private ResponseDefinitionBuilder uploadCompleteGetResumablePositionResponse()
    {
        return aResponse()
                .withStatus( HTTP_CREATED );
    }

    private MappingBuilder getResumablePositionRequest( long sourceLength, String uploadLocationPath )
    {
        return put( urlEqualTo( uploadLocationPath ) )
                .withHeader( "Content-Length", equalTo( "0" ) )
                .withHeader( "Content-Range", equalTo( "bytes */" + sourceLength ) );
    }

    private Path createDump() throws IOException
    {
        Path file = directory.file( "something" );
        Files.createFile( file );
        Files.write( file, "this is simply some weird dump data, but may do the trick for this test of uploading it".getBytes() );
        return file;
    }

    private void authenticateAndCopy( PushToCloudCommand.Copier copier, Path path, long databaseSize, boolean sourceProvided, String username, char[] password )
            throws CommandFailedException, IOException
    {
        String bearerToken = copier.authenticate( false, TEST_CONSOLE_URL, username, password, false );
        PushToCloudCommand.Source source = new PushToCloudCommand.Source( path, databaseSize );
        copier.copy( true, TEST_CONSOLE_URL, "bolt+routing://deadbeef.databases.neo4j.io", source, sourceProvided, bearerToken );
    }

    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    private static class ControlledProgressListener extends ProgressListener.Adapter
    {
        long progress;
        boolean doneCalled;

        @Override
        public void started( String task )
        {
        }

        @Override
        public void started()
        {
        }

        @Override
        public void add( long progress )
        {
            this.progress += progress;
        }

        @Override
        public void done()
        {
            doneCalled = true;
        }

        @Override
        public void failed( Throwable e )
        {
            throw new UnsupportedOperationException( "Should not be called" );
        }
    }
}
