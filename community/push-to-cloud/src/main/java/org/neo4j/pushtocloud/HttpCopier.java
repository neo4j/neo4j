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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.lang.Long.min;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.compress.utils.IOUtils.toByteArray;
import static org.neo4j.pushtocloud.PushToCloudCommand.bytesToGibibytes;

public class HttpCopier implements PushToCloudCommand.Copier
{
    static final int HTTP_RESUME_INCOMPLETE = 308;
    static final int HTTP_UNPROCESSABLE_ENTITY = 422;
    static final int HTTP_TOO_MANY_REQUESTS = 429;
    static final String ERROR_REASON_UNSUPPORTED_INDEXES = "LegacyIndexes";
    static final String ERROR_REASON_EXCEEDS_MAX_SIZE = "ImportExceedsMaxSize";
    private static final long POSITION_UPLOAD_COMPLETED = -1;
    private static final long DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS = SECONDS.toMillis( 64 );
    private static final long DEFAULT_MAXIMUM_RETRIES = 50;
    public static final String UNEXPECTED_RESPONSE_ERROR = "Got unexpected response uploading to storage";

    private final ExecutionContext ctx;
    private final Sleeper sleeper;
    private final ProgressListenerFactory progressListenerFactory;
    private final SystemNanoClock clock;
    private final long maxResumeUploadRetries;
    private final long maximumBackoff;

    HttpCopier( ExecutionContext ctx )
    {
        this(
                ctx,
                Thread::sleep,
                ( text, length ) -> ProgressMonitorFactory.textual(ctx.out()).singlePart(text, length),
                Clocks.nanoClock(),
                DEFAULT_MAXIMUM_RETRIES,
                DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS );
    }

    HttpCopier( ExecutionContext ctx, SystemNanoClock clock )
    {
        this(
                ctx,
                Thread::sleep,
                ( text, length ) -> ProgressMonitorFactory.textual(ctx.out()).singlePart(text, length),
                clock,
                DEFAULT_MAXIMUM_RETRIES,
                DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS );
    }

    HttpCopier( ExecutionContext ctx, long maximumRetries, long maximumBackoff )
    {
        this(
                ctx,
                Thread::sleep,
                ( text, length ) -> ProgressMonitorFactory.textual(ctx.out()).singlePart(text, length),
                Clocks.nanoClock(),
                maximumRetries,
                maximumBackoff );
    }

    HttpCopier( ExecutionContext ctx, Sleeper sleeper, ProgressListenerFactory progressListenerFactory )
    {
        this(
                ctx,
                sleeper,
                progressListenerFactory,
                Clocks.nanoClock(),
                DEFAULT_MAXIMUM_RETRIES,
                DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS );
    }

    HttpCopier(
            ExecutionContext ctx,
            Sleeper sleeper,
            ProgressListenerFactory progressListenerFactory,
            SystemNanoClock clock,
            long maxResumeUploadRetries,
            long maximumBackoff )
    {
        this.ctx = ctx;
        this.sleeper = sleeper;
        this.progressListenerFactory = progressListenerFactory;
        this.clock = clock;
        this.maxResumeUploadRetries = maxResumeUploadRetries;
        this.maximumBackoff = maximumBackoff;
    }

    private static void safeSkip( InputStream sourceStream, long position ) throws IOException
    {
        long toSkip = position;
        while ( toSkip > 0 )
        {
            toSkip -= sourceStream.skip( position );
        }
    }

    /**
     * Parses a response from asking about how far an upload has gone, i.e. how many bytes of the source file have been uploaded. The range is in the format:
     * "bytes=x-y" and since we always ask from 0 then we're interested in y, more specifically y+1 since x-y means that bytes in the range x-y have been
     * received so we want to start sending from y+1.
     */
    private static long parseResumablePosition( String range )
    {
        int dashIndex = range.indexOf( '-' );
        if ( !range.startsWith( "bytes=" ) || dashIndex == -1 )
        {
            throw new CommandFailedException( "Unexpected response when asking where to resume upload from. got '" + range + "'" );
        }
        return Long.parseLong( range.substring( dashIndex + 1 ) ) + 1;
    }

    private static String base64Encode( String username, char[] password )
    {
        String plainToken = username + ':' + String.valueOf( password );
        return Base64.getEncoder().encodeToString( plainToken.getBytes() );
    }

    private static URL safeUrl( String urlString )
    {
        try
        {
            return new URL( urlString );
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException( "Malformed URL '" + urlString + "'", e );
        }
    }

    /**
     * Use the Jackson JSON parser because Neo4j Server depends on this library already and therefore already exists in the environment. This means that this
     * command can parse JSON w/o any additional external dependency and doesn't even need to depend on java 8, where the Rhino script engine has built-in JSON
     * parsing support.
     */
    private static <T> T parseJsonUsingJacksonParser( String json, Class<T> type ) throws IOException
    {
        return new ObjectMapper().readValue( json, type );
    }

    /**
     * Do the actual transfer of the source (a Neo4j database dump) to the target.
     */
    @Override
    public void copy( boolean verbose, String consoleURL, String boltUri, PushToCloudCommand.Source source, boolean deleteSourceAfterImport,
                      String bearerToken )
    {
        String version = getClass().getPackage().getImplementationVersion();
        String bearerTokenHeader = "Bearer " + bearerToken;
        try
        {
            long crc32Sum = source.crc32Sum();
            URL signedURL = retryOnUnavailable( () -> initiateCopy( verbose, safeUrl( consoleURL + "/import" ),
                    crc32Sum, source.size(), bearerTokenHeader, version ) );
            URL uploadLocation = retryOnUnavailable( () -> initiateResumableUpload( verbose, signedURL ) );
            long sourceLength = ctx.fs().getFileSize( source.path() );

            // Enter the resume:able upload loop
            long position = 0;
            int resumeUploadRetries = 0;
            ThreadLocalRandom random = ThreadLocalRandom.current();
            ProgressTrackingOutputStream.Progress
                    uploadProgress = new ProgressTrackingOutputStream.Progress( progressListenerFactory.create( "Upload", sourceLength ), position );
            while ( !resumeUpload( verbose, source.path(), boltUri, sourceLength, position, uploadLocation, uploadProgress ) )
            {
                position = getResumablePosition( verbose, sourceLength, uploadLocation );
                if ( position == POSITION_UPLOAD_COMPLETED )
                {
                    // This is somewhat unexpected, we didn't get an OK from the upload, but when we asked about how far the upload
                    // got it responded that it was fully uploaded. I'd guess we're fine here.
                    break;
                }

                // Truncated exponential backoff
                if ( resumeUploadRetries > maxResumeUploadRetries )
                {
                    throw new CommandFailedException( "Upload failed after numerous attempts." );
                }
                long backoffFromRetryCount = SECONDS.toMillis( 1 << resumeUploadRetries++ ) + random.nextInt( 1_000 );
                sleeper.sleep( min( backoffFromRetryCount, maximumBackoff ) );
            }
            uploadProgress.done();

            triggerImportProtocol( verbose, safeUrl( consoleURL + "/import/upload-complete" ), boltUri, source.path(), crc32Sum, bearerTokenHeader );

            doStatusPolling( verbose, consoleURL, bearerToken, sourceLength );

            if ( deleteSourceAfterImport )
            {
                Files.delete( source.path() );
            }
            else
            {
                ctx.out().printf( "It is safe to delete the dump file now: %s%n", source.path().toAbsolutePath() );
            }
        }
        catch ( InterruptedException | IOException e )
        {
            throw new CommandFailedException( e.getMessage(), e );
        }
    }

    @Override
    public void checkSize( boolean verbose, String consoleURL, long size, String bearerToken )
    {
        retryOnUnavailable( () ->
                            {
                                doCheckSize( verbose, consoleURL, size, bearerToken );
                                return null;
                            } );
    }

    static class RetryableHttpException extends RuntimeException
    {
        RetryableHttpException( CommandFailedException e )
        {
            super( e );
        }
    }

    private interface IOExceptionSupplier<T>
    {
        T get() throws IOException;
    }

    <T> T retryOnUnavailable( IOExceptionSupplier<T> runnableCommand )
    {
        int attempt = 0;
        RetryableHttpException lastException = null;
        while ( true )
        {
            try
            {
                return runnableCommand.get();
            }
            catch ( RetryableHttpException e )
            {
                if ( attempt >= maxResumeUploadRetries ) // Will retry one more, so in the end we have 1 + (n+1) retries
                {
                    break;
                }
                // Truncated exponential backoff
                ThreadLocalRandom random = ThreadLocalRandom.current();
                long backoffFromRetryCount = SECONDS.toMillis( 1 << attempt++ ) + random.nextInt( 1_000 );
                try
                {
                    sleeper.sleep( min( backoffFromRetryCount, maximumBackoff ) );
                }
                catch ( InterruptedException ex )
                {
                    throw new CommandFailedException( e.getMessage(), e );
                }
                lastException = e;
            }
            catch ( IOException e )
            {
                throw new CommandFailedException( e.getMessage(), e );
            }
        }

        throw (RuntimeException) lastException.getCause();
    }

    private void doCheckSize( boolean verbose, String consoleURL, long size, String bearerToken ) throws IOException
    {
        URL url = safeUrl( consoleURL + "/import/size" );
        String bearerTokenHeader = "Bearer " + bearerToken;
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try ( Closeable c = connection::disconnect )
        {
            connection.setDoOutput( true );
            connection.setRequestMethod( "POST" );
            connection.setRequestProperty( "Authorization", bearerTokenHeader );
            connection.setRequestProperty( "Content-Type", "application/json" );
            try ( OutputStream postData = connection.getOutputStream() )
            {
                postData.write( String.format( "{\"FullSize\":%d}", size ).getBytes( UTF_8 ) );
            }
            int responseCode = connection.getResponseCode();
            switch ( responseCode )
            {
            case HTTP_UNPROCESSABLE_ENTITY:
                throw validationFailureErrorResponse( verbose, connection, size );
            case HTTP_OK:
                return;
            case HTTP_GATEWAY_TIMEOUT:
            case HTTP_BAD_GATEWAY:
            case HTTP_UNAVAILABLE:
                throw new RetryableHttpException( unexpectedResponse( verbose, connection, "Size check" ) );
            default:
                throw unexpectedResponse( verbose, connection, "Size check" );
            }
        }
    }

    private void throwIfImportDidNotStart( long importStartedTimeout )
    {
        boolean passedStartImportTimeout = this.clock.millis() > importStartedTimeout;
        if ( passedStartImportTimeout )
        {
            throw new CommandFailedException(
                    "Timed out waiting for database load to start as the database did not enter "
                            + "'loading' state in time. Please retry the operation. You might find more information about the "
                            + "failure on the database status page in https://console.neo4j.io." );
        }
    }

    private void doStatusPolling( boolean verbose, String consoleURL, String bearerToken, long fileSize )
            throws InterruptedException
    {
        ctx.out().println( "We have received your export and it is currently being loaded into your Aura instance." );
        ctx.out().println( "You can wait here, or abort this command and head over to the console to be notified of when your database is running." );
        String bearerTokenHeader = "Bearer " + bearerToken;
        ProgressTrackingOutputStream.Progress statusProgress =
                new ProgressTrackingOutputStream.Progress(
                        progressListenerFactory.create( "Import progress (estimated)", 100L ), 0 );
        boolean importHasStarted = false;
        long importStarted = this.clock.millis();
        double importTimeEstimateMinutes = 5 + (3 * bytesToGibibytes( fileSize ));
        long importTimeEstimateMillis = TimeUnit.SECONDS.toMillis( (long) (importTimeEstimateMinutes * 60) );
        long importStartedTimeout = importStarted + 90 * 1000; // timeout to switch from first running to loading = 1.5 minute
        debug( verbose, format(
                "Rough guess for how long dump file import will take: %.0f minutes; file size is %.1f GB (%d bytes)",
                importTimeEstimateMinutes, bytesToGibibytes( fileSize ), fileSize ) );
        while ( !statusProgress.isDone() )
        {
            StatusBody statusBody = getDatabaseStatus( verbose, safeUrl( consoleURL + "/import/status" ), bearerTokenHeader );
            switch ( statusBody.Status )
            {
            case "running":
                    // It could happen that the very first call of this method is so fast, that the database is still in
                    // state
                    // "running". So we need to check if this is the case and ignore the result in that case and only
                    // take this result as valid, once the status loading or restoring was seen before.
                    if ( importHasStarted )
                    {
                        statusProgress.rewindTo( 0 );
                        statusProgress.add( 100 );
                        statusProgress.done();
                    }
                else
                {
                        throwIfImportDidNotStart(importStartedTimeout);
                }
                break;
            case "loading failed":
                    if ( importHasStarted )
                    {
                        throw formatCommandFailedExceptionError(
                                statusBody.Error.getMessage(), statusBody.Error.getUrl());
                    }
                    else
                    {
                        throwIfImportDidNotStart(importStartedTimeout);
                    }
                    break;
            default:
                    importHasStarted = true;
                    long elapsed = this.clock.millis() - importStarted;
                statusProgress.rewindTo( 0 );
                statusProgress.add( importStatusProgressEstimate( statusBody.Status, elapsed, importTimeEstimateMillis ) );
                break;
            }
            sleeper.sleep( 2000 );
        }
        ctx.out().println( "Your data was successfully pushed to Aura and is now running." );
        long importDurationMillis = this.clock.millis() - importStarted;
        debug( verbose, format( "Import took about %d minutes to complete excluding upload (%d ms)",
                                TimeUnit.MILLISECONDS.toMinutes( importDurationMillis ), importDurationMillis ) );
    }

    int importStatusProgressEstimate( String databaseStatus, long elapsed, long importTimeEstimateMillis )
    {
        switch ( databaseStatus )
        {
        case "running":
            return 0;
        case "updating":
        case "loading":
            int loadProgressEstimation = (int) Math.min( 98, (elapsed * 98) / importTimeEstimateMillis );
            return 1 + loadProgressEstimation;
        default:
            throw new CommandFailedException( String.format(
                    "We're sorry, something has failed during the loading of your database. "
                    + "Please try again and if this problem persists, please open up a support case at https://support.neo4j.com. Database status: %s",
                    databaseStatus ) );
        }
    }

    @Override
    public String authenticate( boolean verbose, String consoleUrl, String username, char[] password, boolean consentConfirmed )
    {
        return retryOnUnavailable( () -> doAuthenticate( verbose, consoleUrl, username, password, consentConfirmed ) );
    }

    private String doAuthenticate( boolean verbose, String consoleUrl, String username, char[] password, boolean consentConfirmed )
            throws IOException
    {
        URL url = safeUrl( consoleUrl + "/import/auth" );
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try ( Closeable c = connection::disconnect )
        {
            connection.setRequestMethod( "POST" );
            connection.setRequestProperty( "Authorization", "Basic " + base64Encode( username, password ) );
            connection.setRequestProperty( "Accept", "application/json" );
            connection.setRequestProperty( "Confirmed", String.valueOf( consentConfirmed ) );
            int responseCode = connection.getResponseCode();
            switch ( responseCode )
            {
            case HTTP_NOT_FOUND:
                throw errorResponse( verbose, connection, "We encountered a problem while contacting your Neo4j Aura instance, " +
                                                          "please check your Bolt URI" );
            case HTTP_MOVED_PERM:
                throw updatePluginErrorResponse( connection );
            case HTTP_UNAUTHORIZED:
                throw errorResponse( verbose, connection, "Invalid username/password credentials" );
            case HTTP_FORBIDDEN:
                throw errorResponse( verbose, connection, "The credentials provided do not give administrative access to the target database" );
            case HTTP_CONFLICT:
                throw errorResponse( verbose, connection, "No consent to overwrite database. Aborting" );
            case HTTP_GATEWAY_TIMEOUT:
            case HTTP_BAD_GATEWAY:
            case HTTP_UNAVAILABLE:
                throw new RetryableHttpException( unexpectedResponse( verbose, connection, "Authorization" ) );
            case HTTP_OK:
                try ( InputStream responseData = connection.getInputStream() )
                {
                    String json = new String( toByteArray( responseData ), UTF_8 );
                    return parseJsonUsingJacksonParser( json, TokenBody.class ).Token;
                }
            default:
                throw unexpectedResponse( verbose, connection, "Authorization" );
            }
        }
    }

    /**
     * Communication with Neo4j's cloud console, resulting in some signed URI to do the actual upload to.
     */
    private URL initiateCopy( boolean verbose, URL importURL, long crc32Sum, long size, String bearerToken, String version ) throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) importURL.openConnection();
        try ( Closeable c = connection::disconnect )
        {
            // POST the request
            connection.setRequestMethod( "POST" );
            connection.setRequestProperty( "Content-Type", "application/json" );
            connection.setRequestProperty( "Authorization", bearerToken );
            connection.setRequestProperty( "Accept", "application/json" );
            connection.setRequestProperty( "Neo4j-Version", version );
            connection.setDoOutput( true );

            try ( OutputStream postData = connection.getOutputStream() )
            {
                postData.write( String.format( "{\"Crc32\":%d, \"FullSize\":%d}", crc32Sum, size ).getBytes( UTF_8 ) );
            }

            // Read the response
            int responseCode = connection.getResponseCode();

            switch ( responseCode )
            {
            case HTTP_NOT_FOUND:
                // fallthrough
            case HTTP_MOVED_PERM:
                throw updatePluginErrorResponse( connection );
            case HTTP_UNAUTHORIZED:
                throw errorResponse( verbose, connection, "The given authorization token is invalid or has expired" );
            case HTTP_UNPROCESSABLE_ENTITY:
                throw validationFailureErrorResponse( verbose, connection, size );
            case HTTP_GATEWAY_TIMEOUT:
            case HTTP_BAD_GATEWAY:
            case HTTP_UNAVAILABLE:
                throw new RetryableHttpException( unexpectedResponse( verbose, connection, "Initiating upload target" ) );
            case HTTP_ACCEPTED:
                // the import request was accepted, and the server has not seen this dump file, meaning the import request is a new operation.
                return safeUrl( extractSignedURIFromResponse( connection ) );
            default:
                throw unexpectedResponse( verbose, connection, "Initiating upload target" );
            }
        }
    }

    /**
     * Makes initial contact with the signed URL we got back when talking to the Neo4j cloud console. This will create yet another URL which will be used to
     * upload the source to, potentially resumed if it gets interrupted in the middle.
     */
    private URL initiateResumableUpload( boolean verbose, URL signedURL ) throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) signedURL.openConnection();
        try ( Closeable c = connection::disconnect )
        {

            connection.setRequestMethod( "POST" );
            connection.setRequestProperty( "Content-Length", "0" );
            connection.setFixedLengthStreamingMode( 0 );
            connection.setRequestProperty( "x-goog-resumable", "start" );
            // We don't want to have any Content-Type set really, but there's this issue with the standard HttpURLConnection
            // implementation where it defaults Content-Type to application/x-www-form-urlencoded for POSTs for some reason
            connection.setRequestProperty( "Content-Type", "" );
            connection.setDoOutput( true );
            int responseCode = connection.getResponseCode();
            switch ( responseCode )
            {
            case HTTP_CREATED:
                return safeUrl( connection.getHeaderField( "Location" ) );
            case HTTP_GATEWAY_TIMEOUT:
            case HTTP_BAD_GATEWAY:
            case HTTP_UNAVAILABLE:
                throw new RetryableHttpException( unexpectedResponse( verbose, connection, "Initiating database upload" ) );
            default:
                throw unexpectedResponse( verbose, connection, "Initiating database upload" );
            }
        }
    }

    /**
     * Uploads source from the given position to the upload location.
     */
    private boolean resumeUpload( boolean verbose, Path source, String boltUri, long sourceLength, long position, URL uploadLocation,
                                  ProgressTrackingOutputStream.Progress uploadProgress )
            throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) uploadLocation.openConnection();
        try ( Closeable c = connection::disconnect )
        {
            connection.setRequestMethod( "PUT" );
            long contentLength = sourceLength - position;
            connection.setRequestProperty( "Content-Length", String.valueOf( contentLength ) );
            connection.setFixedLengthStreamingMode( contentLength );
            if ( position > 0 )
            {
                // If we're not starting from the beginning we need to specify what range we're uploading in this format
                connection.setRequestProperty( "Content-Range", format( "bytes %d-%d/%d", position, sourceLength - 1, sourceLength ) );
            }
            connection.setDoOutput( true );
            uploadProgress.rewindTo( position );
            try ( InputStream sourceStream = Files.newInputStream( source );
                  OutputStream targetStream = connection.getOutputStream() )
            {
                safeSkip( sourceStream, position );
                IOUtils.copy( new BufferedInputStream( sourceStream ), new ProgressTrackingOutputStream( targetStream, uploadProgress ) );
            }
            int responseCode = connection.getResponseCode();
            switch ( responseCode )
            {
            case HTTP_OK:
                return true; // the file is now uploaded, all good
            case HTTP_INTERNAL_ERROR:
            case HTTP_UNAVAILABLE:
            case HTTP_BAD_GATEWAY:
            case HTTP_GATEWAY_TIMEOUT:
                debugErrorResponse( verbose, connection );
                return false;
            case HTTP_FORBIDDEN:
                if ( canSkipToImport( connection.getErrorStream() ) )
                {
                    return true;
                }
            default:
                throw resumePossibleErrorResponse( connection, source, boltUri );
            }
        }
    }

    public boolean canSkipToImport( InputStream errorStream ) throws IOException
    {
        String responseString;
        responseString = new String( toByteArray( errorStream ), UTF_8 );
        try
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

            // Security: Java XML parser has external entities enabled by default.
            // https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html#java
            dbf.setFeature( "http://apache.org/xml/features/disallow-doctype-decl", true );
            dbf.setXIncludeAware( false );

            DocumentBuilder builder = dbf.newDocumentBuilder();
            Document document = builder.parse( new InputSource( new StringReader( responseString ) ) );
            document.getDocumentElement().normalize();
            Node codeNode = document.getElementsByTagName( "Code" ).item( 0 );
            Node detailsNode = document.getElementsByTagName( "Details" ).item( 0 );

            if ( isNull( codeNode, detailsNode ) )
            {
                return false;
            }
            String code = codeNode.getTextContent();
            String details = detailsNode.getTextContent();
            if ( isNull( code, details ) )
            {
                return false;
            }

            String objectExistsText = "does not have storage.objects.delete access to the Google Cloud Storage object.";
            boolean valid = details.contains( objectExistsText ) && code.equals( "AccessDenied" );

            if ( !valid )
            {
                ctx.out().println( UNEXPECTED_RESPONSE_ERROR );
                return false;
            }
            else
            {
                ctx.out().println( "Detected already uploaded object, proceeding to import" );
                return true;
            }

        }
        catch ( ParserConfigurationException | SAXException | DOMException e )
        {
            throw new IOException( "Encountered invalid response from cloud import location" , e.getCause() );
        }
    }

    private boolean isNull( Object codeNode, Object detailsNode )
    {
        if ( codeNode == null || detailsNode == null )
        {
            ctx.out().println( UNEXPECTED_RESPONSE_ERROR );
            return true;
        }
        return false;
    }

    private void triggerImportProtocol( boolean verbose, URL importURL, String boltUri, Path source, long crc32Sum, String bearerToken )
            throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) importURL.openConnection();
        try ( Closeable c = connection::disconnect )
        {
            connection.setRequestMethod( "POST" );
            connection.setRequestProperty( "Content-Type", "application/json" );
            connection.setRequestProperty( "Authorization", bearerToken );
            connection.setDoOutput( true );
            try ( OutputStream postData = connection.getOutputStream() )
            {
                postData.write( String.format( "{\"Crc32\":%d}", crc32Sum ).getBytes( UTF_8 ) );
            }

            int responseCode = connection.getResponseCode();
            switch ( responseCode )
            {
            case HTTP_NOT_FOUND:
                // fallthrough
            case HTTP_MOVED_PERM:
                throw updatePluginErrorResponse( connection );
            case HTTP_TOO_MANY_REQUESTS:
                throw resumePossibleErrorResponse( connection, source, boltUri );
            case HTTP_CONFLICT:
                throw errorResponse( verbose, connection,
                                     "The target database contained data and consent to overwrite the data was not given. Aborting" );
            case HTTP_OK:
                // All good, we managed to trigger the import protocol after our completed upload
                break;
            default:
                throw resumePossibleErrorResponse( connection, source, boltUri );
            }
        }
    }

    private StatusBody getDatabaseStatus( boolean verbose, URL statusURL, String bearerToken )
    {
        return retryOnUnavailable( () -> doGetDatabaseStatus( verbose, statusURL, bearerToken ) );
    }

    private StatusBody doGetDatabaseStatus( boolean verbose, URL statusURL, String bearerToken ) throws  IOException
    {
        HttpURLConnection connection = (HttpURLConnection) statusURL.openConnection();
        try ( Closeable c = connection::disconnect )
        {
            connection.setRequestMethod( "GET" );
            connection.setRequestProperty( "Authorization", bearerToken );
            connection.setDoOutput( true );

            int responseCode = connection.getResponseCode();
            switch ( responseCode )
            {
            case HTTP_NOT_FOUND:
                // fallthrough
            case HTTP_MOVED_PERM:
                throw updatePluginErrorResponse( connection );
            case HTTP_OK:
                try ( InputStream responseData = connection.getInputStream() )
                {
                    String json = new String( toByteArray( responseData ), UTF_8 );
                    //debugResponse( verbose, json, connection, false );
                    return parseJsonUsingJacksonParser( json, StatusBody.class );
                }
            case HTTP_GATEWAY_TIMEOUT:
            case HTTP_BAD_GATEWAY:
            case HTTP_UNAVAILABLE:
                throw new RetryableHttpException( unexpectedResponse( verbose, connection, "Trigger import/restore after successful upload" ) );
            default:
                throw unexpectedResponse( verbose, connection, "Trigger import/restore after successful upload" );
            }
        }
    }

    /**
     * Asks about how far the upload has gone so far, typically after being interrupted one way or another. The result of this method can be fed into {@link
     * #resumeUpload(boolean, Path, String, long, long, URL, ProgressTrackingOutputStream.Progress)} to resume an upload.
     */
    private long getResumablePosition( boolean verbose, long sourceLength, URL uploadLocation ) throws IOException
    {
        HttpURLConnection connection = (HttpURLConnection) uploadLocation.openConnection();
        try ( Closeable c = connection::disconnect )
        {
            debug( verbose, "Asking about resumable position for the upload" );
            connection.setRequestMethod( "PUT" );
            connection.setRequestProperty( "Content-Length", "0" );
            connection.setFixedLengthStreamingMode( 0 );
            connection.setRequestProperty( "Content-Range", "bytes */" + sourceLength );
            connection.setDoOutput( true );
            int responseCode = connection.getResponseCode();
            switch ( responseCode )
            {
            case HTTP_OK:
            case HTTP_CREATED:
                debug( verbose, "Upload seems to be completed got " + responseCode );
                return POSITION_UPLOAD_COMPLETED;
            case HTTP_GATEWAY_TIMEOUT:
            case HTTP_BAD_GATEWAY:
            case HTTP_UNAVAILABLE:
                throw new RetryableHttpException( unexpectedResponse( verbose, connection, "Acquire resumable upload position" ) );
            case HTTP_RESUME_INCOMPLETE:
                String range = connection.getHeaderField( "Range" );
                debug( verbose, "Upload not completed got " + range );
                long position = range == null ? 0 // No bytes have been received at all, so let's return position 0, i.e. from the beginning of the file
                                              : parseResumablePosition( range );
                debug( verbose, "Parsed that as position " + position );
                return position;
            default:
                throw unexpectedResponse( verbose, connection, "Acquire resumable upload position" );
            }
        }
    }

    private String extractSignedURIFromResponse( HttpURLConnection connection ) throws IOException
    {
        try ( InputStream responseData = connection.getInputStream() )
        {
            String json = new String( toByteArray( responseData ), UTF_8 );
            return parseJsonUsingJacksonParser( json, SignedURIBody.class ).SignedURI;
        }
    }

    private void debug( boolean verbose, String string )
    {
        if ( verbose )
        {
            ctx.out().println( string );
        }
    }

    private void debugErrorResponse( boolean verbose, HttpURLConnection connection ) throws IOException
    {
        if ( verbose )
        {
            String responseString;
            try ( InputStream responseData = connection.getErrorStream() )
            {
                responseString = new String( toByteArray( responseData ), UTF_8 );
            }
            debugResponse( true, responseString, connection, true );
        }
    }

    private void debugResponse( boolean verbose, String responseBody, HttpURLConnection connection, boolean error ) throws IOException
    {
        if ( verbose )
        {
            debug( true, error ? "=== Unexpected response ===" : "=== Response ===" );
            debug( true, "Response message: " + connection.getResponseMessage() );
            debug( true, "Response headers:" );
            connection.getHeaderFields().forEach( ( key, value1 ) ->
            {
                for ( String value : value1 )
                {
                    debug( true, "  " + key + ": " + value );
                }
            } );
            debug( true, "Response data: " + responseBody );
        }
    }

    private CommandFailedException errorResponse( boolean verbose, HttpURLConnection connection, String errorDescription ) throws IOException
    {
        debugErrorResponse( verbose, connection );
        return new CommandFailedException( errorDescription );
    }

    private CommandFailedException resumePossibleErrorResponse( HttpURLConnection connection, Path dump, String boltUri ) throws IOException
    {
        debugErrorResponse( true, connection );
        return new CommandFailedException( "We encountered a problem while communicating to the Neo4j Aura system. \n" +
                                           "You can re-try using the existing dump by running this command: \n" +
                                           String.format( "neo4j-admin push-to-cloud --%s=%s --%s=%s", "dump", dump.toAbsolutePath(), "bolt-uri",
                                                          boltUri ) );
    }

    private CommandFailedException updatePluginErrorResponse( HttpURLConnection connection ) throws IOException
    {
        debugErrorResponse( true, connection );
        return new CommandFailedException(
                "We encountered a problem while communicating to the Neo4j Aura system. " +
                "Please check that you are using the latest version of the push-to-cloud plugin and upgrade if necessary. " +
                "If this problem persists after upgrading, please contact support at: https://support.neo4j.com " +
                        "and attach the logs shown below to your ticket in the support portal." );
    }

    private CommandFailedException validationFailureErrorResponse( boolean verbose, HttpURLConnection connection, long size )
            throws IOException
    {
        try ( InputStream responseData = connection.getErrorStream() )
        {
            String responseString = new String( toByteArray( responseData ), UTF_8 );
            debugResponse( verbose, responseString, connection, true );
            ErrorBody errorBody = parseJsonUsingJacksonParser( responseString, ErrorBody.class );

            String message = errorBody.getMessage();

            // No special treatment required
            if ( ERROR_REASON_EXCEEDS_MAX_SIZE.equals( errorBody.getReason() ) )
            {
                String trimmedMessage = StringUtils.removeEnd( message, "." );
                message = format( "%s. Minimum storage space required: %s", trimmedMessage, PushToCloudCommand.sizeText( size ) );
            }

            return formatCommandFailedExceptionError( message, errorBody.getUrl() );
        }
    }

    private CommandFailedException unexpectedResponse( boolean verbose, HttpURLConnection connection, String requestDescription ) throws IOException
    {
        return errorResponse( verbose, connection, format( "Unexpected response code %d from request: %s", connection.getResponseCode(), requestDescription ) );
    }

    private CommandFailedException formatCommandFailedExceptionError( String message, String url )
    {
        if ( StringUtils.isEmpty( url ) )
        {
            return new CommandFailedException( message );
        }
        else
        {
            String trimmedMessage = StringUtils.removeEnd( message, "." );
            return new CommandFailedException( format( "Error: %s. See: %s", trimmedMessage, url ) );
        }
    }

    interface Sleeper
    {
        void sleep( long millis ) throws InterruptedException;
    }

    public interface ProgressListenerFactory
    {
        ProgressListener create( String text, long length );
    }

    // Simple structs for mapping JSON to objects, used by the jackson parser which Neo4j happens to depend on anyway
    @JsonIgnoreProperties( ignoreUnknown = true )
    private static class SignedURIBody
    {
        public String SignedURI;
    }

    @JsonIgnoreProperties( ignoreUnknown = true )
    private static class TokenBody
    {
        public String Token;
    }

    @JsonIgnoreProperties( ignoreUnknown = true )
    static class StatusBody
    {
        public String Status;
        public ErrorBody Error = new ErrorBody();
    }

    @JsonIgnoreProperties( ignoreUnknown = true )
    static class ErrorBody
    {
        private static final String DEFAULT_MESSAGE =
                "an unexpected problem ocurred, please contact customer support for assistance";
        private static final String DEFAULT_REASON = "UnknownError";

        private final String message;
        private final String reason;
        private final String url;

        ErrorBody()
        {
            this( null, null, null );
        }

        @JsonCreator
        ErrorBody( @JsonProperty( "Message" ) String message, @JsonProperty( "Reason" ) String reason,
                   @JsonProperty( "Url" ) String url )
        {
            this.message = message;
            this.reason = reason;
            this.url = url;
        }

        public String getMessage()
        {
            return StringUtils.defaultIfBlank( this.message, DEFAULT_MESSAGE );
        }

        public String getReason()
        {
            return StringUtils.defaultIfBlank( this.reason, DEFAULT_REASON );
        }

        public String getUrl()
        {
            return this.url;
        }
    }
}
