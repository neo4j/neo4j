/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.notMatching;
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
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.neo4j.pushtocloud.HttpCopier.HTTP_RESUME_INCOMPLETE;

public class HttpCopierTest
{
    private static final HttpCopier.ProgressListenerFactory NO_OP_PROGRESS = ( name, length ) -> ProgressListener.NONE;

    private static final int TEST_PORT = 8080;
    private static final String TEST_CONSOLE_URL = "http://localhost:" + TEST_PORT;
    private static final String STATUS_POLLING_PASSED_FIRST_CALL = "Passed first";

    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    @Rule
    public WireMockRule wireMock = new WireMockRule( TEST_PORT );
    @Rule
    public TestDirectory directory = TestDirectory.testDirectory();

    @Test
    public void shouldHandleSuccessfulHappyCaseRunThroughOfTheWholeProcess() throws Exception
    {
        // given
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ), millis -> {}, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );

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
        authenticateAndCopy( copier, source, "user", "pass".toCharArray() );

        // then
        verify( postRequestedFor( urlEqualTo( "/import/auth" ) ) );
        verify( postRequestedFor( urlEqualTo( "/import" ) )
                .withRequestBody( matchingJsonPath("DumpSize", equalTo( String.valueOf( sourceLength ) ) ) ) );
        verify( postRequestedFor( urlEqualTo( signedURIPath ) ) );
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) ) );
        verify( postRequestedFor( urlEqualTo( "/import/upload-complete" ) ) );
        assertTrue( progressListener.doneCalled );
        // we need to add 3 to the progress listener because of the database phases
        assertEquals( sourceLength + 3, progressListener.progress );
    }

    @Test
    public void shouldHandleResumableFailureWhileUploading() throws Exception
    {
        // given
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ), millis -> {}, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength ).willReturn( aResponse().withStatus( HttpCopier.HTTP_TOO_MANY_REQUESTS ) ) );

        // when
        assertThrows( CommandFailed.class, containsString( "You can re-try using the existing dump by running this command" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleResumableFailureWhenImportIsTriggered() throws Exception
    {
        // given
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ), millis -> {}, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );

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
        assertThrows( CommandFailed.class, containsString( "You can re-try using the existing dump by running this command" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleBadCredentialsInAuthorizationRequest() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                .withStatus( HTTP_UNAUTHORIZED ) ) );

        // when/then
        assertThrows( CommandFailed.class, CoreMatchers.equalTo( "Invalid username/password credentials" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleAuthenticateMovedRoute() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                .withStatus( HTTP_NOT_FOUND ) ) );

        // when/then
        assertThrows( CommandFailed.class, CoreMatchers.containsString( "please contact support" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleMoveUploadTargetRoute() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );

        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";

        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( "abc" ).willReturn( aResponse()
                .withStatus( HTTP_NOT_FOUND ) ) );

        // when/then
        assertThrows( CommandFailed.class, CoreMatchers.containsString( "please contact support" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleImportRequestMovedRoute() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );

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
        assertThrows( CommandFailed.class, CoreMatchers.containsString( "please contact support" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleInsufficientCredentialsInAuthorizationRequest() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                .withStatus( HTTP_FORBIDDEN ) ) );

        // when/then
        assertThrows( CommandFailed.class, containsString( "administrative access" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleUnexpectedResponseFromAuthorizationRequest() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                .withStatus( HTTP_INTERNAL_ERROR ) ) );

        // when/then
        assertThrows( CommandFailed.class, allOf( containsString( "Unexpected response" ), containsString( "Authorization" ) ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleUnauthorizedResponseFromInitiateUploadTarget() throws IOException
    {
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        String token = "abc";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( token ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( token ).willReturn( aResponse()
                .withStatus( HTTP_UNAUTHORIZED ) ) );

        // when/then
        assertThrows( CommandFailed.class, containsString( "authorization token is invalid" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleConflictResponseFromInitiateUploadTargetAndContinueOnUserConsent() throws IOException, CommandFailed
    {
        ControlledOutsideWorld outsideWorld = new ControlledOutsideWorld( fs );
        outsideWorld.withPromptResponse( "my-username" ); // prompt for username
        outsideWorld.withPasswordResponse( "pass".toCharArray() ); // prompt for password
        outsideWorld.withPromptResponse( "y" ); // prompt for consent to overwrite db
        HttpCopier copier = new HttpCopier( outsideWorld );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        String uploadLocationPath = "/upload";
        wireMock.stubFor( authenticationRequest( true ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( authenticationRequest( false ).willReturn( aResponse()
                .withStatus( HTTP_CONFLICT ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        // and just the rest of the responses so that the upload can continue w/o failing
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( successfulInitiateUploadResponse( uploadLocationPath ) ) );
        wireMock.stubFor( resumeUploadRequest( uploadLocationPath, sourceLength ).willReturn( successfulResumeUploadResponse() ) );
        wireMock.stubFor( triggerImportRequest( authorizationTokenResponse ).willReturn( successfulTriggerImportResponse() ) );
        wireMock.stubFor( firstStatusPollingRequest( authorizationTokenResponse ) );
        wireMock.stubFor( secondStatusPollingRequest( authorizationTokenResponse ) );

        // when
        authenticateAndCopy( copier, source, "user", "pass".toCharArray() );

        // then there should be one request w/o the user consent and then (since the user entered 'y') one w/ user consent
        verify( postRequestedFor( urlEqualTo( "/import/auth" ) ).withHeader("Confirmed", equalTo( "false" ) ) );
        verify( postRequestedFor( urlEqualTo( "/import/auth" ) ).withHeader( "Confirmed", equalTo("true") ) );
    }

    @Test
    public void shouldHandleConflictResponseFromAuthenticationWithoutUserConsent() throws IOException
    {
        ControlledOutsideWorld outsideWorld = new ControlledOutsideWorld( fs );
        outsideWorld.withPromptResponse( "my-username" ); // prompt for username
        outsideWorld.withPromptResponse( "n" ); // prompt for consent to overwrite db
        HttpCopier copier = new HttpCopier( outsideWorld );
        Path source = createDump();
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        wireMock.stubFor( authenticationRequest(false).willReturn( aResponse().withStatus( HTTP_CONFLICT ) ) );
        wireMock.stubFor( authenticationRequest(true).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse)
                .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );

        // when
        assertThrows( CommandFailed.class, containsString( "No consent to overwrite" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );

        // then there should be one request w/o the user consent and then (since the user entered 'y') one w/ user consent
        verify( postRequestedFor( urlEqualTo( "/import/auth" ) ).withHeader("Confirmed", equalTo( "false" ) ) );
        verify( 0, postRequestedFor( urlEqualTo( "/import/auth" ) ).withHeader("Confirmed", equalTo( "true" ) ) );
    }

    @Test
    public void shouldHandleUnexpectedResponseFromInitiateUploadTargetRequest() throws IOException
    {
        ControlledOutsideWorld outsideWorld = new ControlledOutsideWorld( fs );
        outsideWorld.withPromptResponse( "my-username" ); // prompt for username
        outsideWorld.withPromptResponse( "n" ); // prompt for consent to overwrite db
        HttpCopier copier = new HttpCopier( outsideWorld );
        Path source = createDump();
        String authorizationTokenResponse = "abc";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse ).willReturn( aResponse()
                .withStatus( HTTP_BAD_GATEWAY ) ) );

        // when
        assertThrows( CommandFailed.class, allOf( containsString( "Unexpected response" ), containsString( "Initiating upload target" ) ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleInitiateUploadFailure() throws IOException
    {
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        String authorizationTokenResponse = "abc";
        String signedURIPath = "/signed";
        wireMock.stubFor( authenticationRequest( false ).willReturn( successfulAuthorizationResponse( authorizationTokenResponse ) ) );
        wireMock.stubFor( initiateUploadTargetRequest( authorizationTokenResponse )
                .willReturn( successfulInitiateUploadTargetResponse( signedURIPath ) ) );
        wireMock.stubFor( initiateUploadRequest( signedURIPath ).willReturn( aResponse()
                .withStatus( HTTP_INTERNAL_ERROR ) ) );

        // when
        assertThrows( CommandFailed.class, allOf( containsString( "Unexpected response" ), containsString( "Initiating database upload" ) ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldHandleUploadInACoupleOfRounds() throws IOException, CommandFailed
    {
        ControlledProgressListener progressListener = new ControlledProgressListener();
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ), millis -> {}, ( name, length ) -> progressListener );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );
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
        authenticateAndCopy( copier, source, "user", "pass".toCharArray() );

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
        // we need to add 3 to the progress listener because of the database phases
        assertEquals( sourceLength + 3, progressListener.progress );
    }

    @Test
    public void shouldHandleIncompleteUploadButPositionSaysComplete() throws IOException, CommandFailed
    {
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ), millis -> {}, NO_OP_PROGRESS );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );
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
        authenticateAndCopy( copier, source, "user", "pass".toCharArray() );

        // then
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) )
                .withHeader( "Content-Length", equalTo( Long.toString( sourceLength ) ) )
                .withoutHeader( "Content-Range" ) );
        verify( putRequestedFor( urlEqualTo( uploadLocationPath ) )
                .withHeader( "Content-Length", equalTo( "0" ) )
                .withHeader( "Content-Range", equalTo( "bytes */" + sourceLength ) ) );
    }

    @Test
    public void shouldHandleConflictOnTriggerImportAfterUpload() throws IOException
    {
        // given
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ) );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );
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
        assertThrows( CommandFailed.class, containsString( "The target database contained data and consent to overwrite the data was not given." ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
    }

    @Test
    public void shouldBackoffAndFailIfTooManyAttempts() throws IOException, InterruptedException
    {
        // given
        HttpCopier.Sleeper sleeper = mock( HttpCopier.Sleeper.class );
        HttpCopier copier = new HttpCopier( new ControlledOutsideWorld( fs ), sleeper, NO_OP_PROGRESS );
        Path source = createDump();
        long sourceLength = fs.getFileSize( source.toFile() );
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
        assertThrows( CommandFailed.class, containsString( "Upload failed after numerous attempts" ),
                () -> authenticateAndCopy( copier, source, "user", "pass".toCharArray() ) );
        Mockito.verify( sleeper, atLeast( 30 ) ).sleep( anyLong() );
    }

    private MappingBuilder authenticationRequest( boolean userConsent )
    {
        return post( urlEqualTo( "/import/auth" ) )
                .withHeader( "Authorization", matching( "^Basic .*" ) )
                .withHeader( "Accept", equalTo( "application/json" ) )
                .withHeader( "Confirmed", equalTo( userConsent ? "true" : "false" ));
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
        File file = directory.file( "something" );
        assertTrue( file.createNewFile() );
        Files.write( file.toPath(), "this is simply some weird dump data, but may do the trick for this test of uploading it".getBytes() );
        return file.toPath();
    }

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

    private void authenticateAndCopy( PushToCloudCommand.Copier copier, Path source, String username, char[] password ) throws CommandFailed
    {
        String bearerToken = copier.authenticate( false, TEST_CONSOLE_URL, username, password, false );
        copier.copy( false, TEST_CONSOLE_URL, "bolt+routing://deadbeef.databases.neo4j.io", source,  bearerToken );
    }

    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    private static class ControlledProgressListener implements ProgressListener
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
        public void set( long progress )
        {
            throw new UnsupportedOperationException( "Should not be called" );
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
