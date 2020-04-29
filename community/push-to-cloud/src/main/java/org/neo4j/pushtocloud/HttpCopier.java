/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;

import static java.lang.Long.min;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_NOT_ACCEPTABLE;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.compress.utils.IOUtils.toByteArray;

public class HttpCopier implements PushToCloudCommand.Copier
{
    static final int HTTP_RESUME_INCOMPLETE = 308;
    static final int HTTP_UNPROCESSABLE_ENTITY = 422;
    static final int HTTP_TOO_MANY_REQUESTS = 429;
    static final String ERROR_REASON_UNSUPPORTED_INDEXES = "LegacyIndexes";
    static final String ERROR_REASON_EXCEEDS_MAX_SIZE = "ImportExceedsMaxSize";
    private static final long POSITION_UPLOAD_COMPLETED = -1;
    private static final long MAXIMUM_RETRY_BACKOFF = SECONDS.toMillis( 64 );

    private final ExecutionContext ctx;
    private final Sleeper sleeper;
    private final ProgressListenerFactory progressListenerFactory;

    HttpCopier( ExecutionContext ctx )
    {
        this( ctx, Thread::sleep, ( text, length ) -> ProgressMonitorFactory.textual( ctx.out() ).singlePart( text, length ) );
    }

    HttpCopier( ExecutionContext ctx, Sleeper sleeper, ProgressListenerFactory progressListenerFactory )
    {
        this.ctx = ctx;
        this.sleeper = sleeper;
        this.progressListenerFactory = progressListenerFactory;
    }

    private static String buildCrc32WithConsentJson( long crc32Sum, Long fileSize )
    {
        String fileSizeString = "";
        if ( fileSize != null )
        {
            fileSizeString = String.format( ", \"DumpSize\":%d", fileSize );
        }
        return String.format( "{\"Crc32\":%d%s}", crc32Sum, fileSizeString );
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
    private static long parseResumablePosition( String range ) throws CommandFailedException
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
        String plainToken = new StringBuilder( username ).append( ':' ).append( password ).toString();
        return Base64.getEncoder().encodeToString( plainToken.getBytes() );
    }

    private static long calculateCrc32HashOfFile( Path source ) throws IOException
    {
        CRC32 crc = new CRC32();
        try ( InputStream inputStream = new BufferedInputStream( new FileInputStream( source.toFile() ) ) )
        {
            int cnt;
            while ( (cnt = inputStream.read()) != -1 )
            {
                crc.update( cnt );
            }
        }
        return crc.getValue();
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
    public void copy( boolean verbose, String consoleURL, String boltUri, Path source, boolean deleteSourceAfterImport, String bearerToken )
            throws CommandFailedException
    {
        try
        {
            String bearerTokenHeader = "Bearer " + bearerToken;
            long crc32Sum = calculateCrc32HashOfFile( source );
            URL signedURL = initiateCopy( verbose, safeUrl( consoleURL + "/import" ), crc32Sum, source.toFile().length(), bearerTokenHeader );
            URL uploadLocation = initiateResumableUpload( verbose, signedURL );
            long sourceLength = ctx.fs().getFileSize( source.toFile() );

            // Enter the resume:able upload loop
            long position = 0;
            int retries = 0;
            ThreadLocalRandom random = ThreadLocalRandom.current();
            ProgressTrackingOutputStream.Progress
                    uploadProgress = new ProgressTrackingOutputStream.Progress( progressListenerFactory.create( "Upload", sourceLength ), position );
            while ( !resumeUpload( verbose, source, boltUri, sourceLength, position, uploadLocation, uploadProgress ) )
            {
                position = getResumablePosition( verbose, sourceLength, uploadLocation );
                if ( position == POSITION_UPLOAD_COMPLETED )
                {
                    // This is somewhat unexpected, we didn't get an OK from the upload, but when we asked about how far the upload
                    // got it responded that it was fully uploaded. I'd guess we're fine here.
                    break;
                }

                // Truncated exponential backoff
                if ( retries > 50 )
                {
                    throw new CommandFailedException( "Upload failed after numerous attempts." );
                }
                long backoffFromRetryCount = SECONDS.toMillis( 1 << retries++ ) + random.nextInt( 1_000 );
                sleeper.sleep( min( backoffFromRetryCount, MAXIMUM_RETRY_BACKOFF ) );
            }
            uploadProgress.done();

            triggerImportProtocol( verbose, safeUrl( consoleURL + "/import/upload-complete" ), boltUri, source, crc32Sum, bearerTokenHeader );

            doStatusPolling( verbose, consoleURL, bearerToken, sourceLength );

            if ( deleteSourceAfterImport )
            {
                source.toFile().delete();
            }
            else
            {
                ctx.out().println( String.format( "It is safe to delete the dump file now: %s", source.toFile().getAbsolutePath() ) );
            }
        }
        catch ( InterruptedException | IOException e )
        {
            throw new CommandFailedException( e.getMessage(), e );
        }
    }

    @Override
    public void checkSize( boolean verbose, String consoleURL, long sizeInBytes, String bearerToken )
    {
        try
        {
            URL url = safeUrl( consoleURL + "/import/size" );
            String bearerTokenHeader = "Bearer " + bearerToken;
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            try
            {
                connection.setDoOutput( true );
                connection.setRequestMethod( "POST" );
                connection.setRequestProperty( "Authorization", bearerTokenHeader );
                connection.setRequestProperty( "Content-Type", "application/json" );
                try ( OutputStream postData = connection.getOutputStream() )
                {
                    postData.write( String.format( "{\"DumpSize\":%d}", sizeInBytes ).getBytes( UTF_8 ) );
                }
                int responseCode = connection.getResponseCode();
                switch ( responseCode )
                {
                case HTTP_UNPROCESSABLE_ENTITY:
                    throw insufficientSpaceErrorResponse( verbose, connection, sizeInBytes );
                case HTTP_OK:
                    return;
                default:
                    throw unexpectedResponse( verbose, connection, "Size check" );
                }
            }
            finally
            {
                connection.disconnect();
            }
        }
        catch ( IOException e )
        {
            throw new CommandFailedException( e.getMessage(), e );
        }
    }

    private void doStatusPolling( boolean verbose, String consoleURL, String bearerToken, long fileSize )
            throws IOException, InterruptedException, CommandFailedException
    {
        ctx.out().println( "We have received your export and it is currently being loaded into your Aura instance." );
        ctx.out().println( "You can wait here, or abort this command and head over to the console to be notified of when your database is running." );
        String bearerTokenHeader = "Bearer " + bearerToken;
        ProgressTrackingOutputStream.Progress statusProgress =
                new ProgressTrackingOutputStream.Progress(
                        progressListenerFactory.create( "Import progress (estimated)", 100L ), 0 );
        boolean firstRunning = true;
        long importStarted = System.currentTimeMillis();
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
                // It could happen that the very first call of this method is so fast, that the database is still in state
                // "running". So we need to check if this is the case and ignore the result in that case and only
                // take this result as valid, once the status loading or restoring was seen before.
                if ( !firstRunning )
                {
                    statusProgress.rewindTo( 0 );
                    statusProgress.add( 100 );
                    statusProgress.done();
                }
                else
                {
                    boolean passedStartImportTimeout = importStarted > importStartedTimeout;
                    if ( passedStartImportTimeout )
                    {
                        throw new CommandFailedException( "We're sorry, it couldn't be detected that the import was started, " +
                                                          "please check the console for further details." );
                    }
                }
                break;
            case "loading failed":
                throw formatCommandFailedExceptionError( statusBody.Error.getMessage(), statusBody.Error.getUrl() );
            default:
                firstRunning = false;
                long elapsed = System.currentTimeMillis() - importStarted;
                statusProgress.rewindTo( 0 );
                statusProgress.add( importStatusProgressEstimate( statusBody.Status, elapsed, importTimeEstimateMillis ) );
                break;
            }
            sleeper.sleep( 2000 );
        }
        ctx.out().println( "Your data was successfully pushed to Aura and is now running." );
        long importDurationMillis = System.currentTimeMillis() - importStarted;
        debug( verbose, format( "Import took about %d minutes to complete excluding upload (%d ms)",
                                TimeUnit.MILLISECONDS.toMinutes( importDurationMillis ), importDurationMillis ) );
    }

    int importStatusProgressEstimate( String databaseStatus, long elapsed, long importTimeEstimateMillis )
            throws CommandFailedException
    {
        switch ( databaseStatus )
        {
        case "running":
            return 0;
        case "loading":
            int loadProgressEstimation = (int) Math.min( 98, (elapsed * 98) / importTimeEstimateMillis );
            return 1 + loadProgressEstimation;
        default:
            throw new CommandFailedException( String.format(
                    "We're sorry, something has failed during the loading of your database. "
                    + "Please try again and if this problem persists, please open up a support case. Database status: %s",
                    databaseStatus ) );
        }
    }

    @Override
    public String authenticate( boolean verbose, String consoleUrl, String username, char[] password, boolean consentConfirmed ) throws CommandFailedException
    {
        try
        {
            URL url = safeUrl( consoleUrl + "/import/auth" );
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            try
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
            finally
            {
                connection.disconnect();
            }
        }
        catch ( IOException e )
        {
            throw new CommandFailedException( e.getMessage(), e );
        }
    }

    /**
     * Communication with Neo4j's cloud console, resulting in some signed URI to do the actual upload to.
     */
    private URL initiateCopy( boolean verbose, URL importURL, long crc32Sum, long fileSize, String bearerToken )
            throws IOException, CommandFailedException
    {
        HttpURLConnection connection = (HttpURLConnection) importURL.openConnection();
        try
        {
            // POST the request
            connection.setRequestMethod( "POST" );
            connection.setRequestProperty( "Content-Type", "application/json" );
            connection.setRequestProperty( "Authorization", bearerToken );
            connection.setRequestProperty( "Accept", "application/json" );
            connection.setDoOutput( true );
            try ( OutputStream postData = connection.getOutputStream() )
            {
                postData.write( buildCrc32WithConsentJson( crc32Sum, fileSize ).getBytes( UTF_8 ) );
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
                // Deprecated: the use of this status code for the purpose below should be replaced with HTTP_UNPROCESSABLE_ENTITY in a future release.
            case HTTP_NOT_ACCEPTABLE:
                throw insufficientSpaceErrorResponse( verbose, connection, fileSize );
            case HTTP_UNPROCESSABLE_ENTITY:
                throw validationFailureErrorResponse( verbose, connection, fileSize );
            case HTTP_ACCEPTED:
                // the import request was accepted, and the server has not seen this dump file, meaning the import request is a new operation.
                return safeUrl( extractSignedURIFromResponse( verbose, connection ) );
            default:
                throw unexpectedResponse( verbose, connection, "Initiating upload target" );
            }
        }
        finally
        {
            connection.disconnect();
        }
    }

    /**
     * Makes initial contact with the signed URL we got back when talking to the Neo4j cloud console. This will create yet another URL which will be used to
     * upload the source to, potentially resumed if it gets interrupted in the middle.
     */
    private URL initiateResumableUpload( boolean verbose, URL signedURL ) throws IOException, CommandFailedException
    {
        HttpURLConnection connection = (HttpURLConnection) signedURL.openConnection();
        try
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
            if ( responseCode != HTTP_CREATED )
            {
                throw unexpectedResponse( verbose, connection, "Initiating database upload" );
            }
            return safeUrl( connection.getHeaderField( "Location" ) );
        }
        finally
        {
            connection.disconnect();
        }
    }

    /**
     * Uploads source from the given position to the upload location.
     */
    private boolean resumeUpload( boolean verbose, Path source, String boltUri, long sourceLength, long position, URL uploadLocation,
                                  ProgressTrackingOutputStream.Progress uploadProgress )
            throws IOException, CommandFailedException
    {
        HttpURLConnection connection = (HttpURLConnection) uploadLocation.openConnection();
        try
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
            try ( InputStream sourceStream = new FileInputStream( source.toFile() );
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
                debugErrorResponse( verbose, connection );
                return false;
            default:
                throw resumePossibleErrorResponse( connection, source, boltUri );
            }
        }
        finally
        {
            connection.disconnect();
        }
    }

    private void triggerImportProtocol( boolean verbose, URL importURL, String boltUri, Path source, long crc32Sum, String bearerToken )
            throws IOException, CommandFailedException
    {
        HttpURLConnection connection = (HttpURLConnection) importURL.openConnection();
        try
        {
            connection.setRequestMethod( "POST" );
            connection.setRequestProperty( "Content-Type", "application/json" );
            connection.setRequestProperty( "Authorization", bearerToken );
            connection.setDoOutput( true );
            try ( OutputStream postData = connection.getOutputStream() )
            {
                postData.write( buildCrc32WithConsentJson( crc32Sum, null ).getBytes( UTF_8 ) );
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
        finally
        {
            connection.disconnect();
        }
    }

    private StatusBody getDatabaseStatus( boolean verbose, URL statusURL, String bearerToken )
            throws IOException, CommandFailedException
    {
        HttpURLConnection connection = (HttpURLConnection) statusURL.openConnection();
        try
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
                    return parseJsonUsingJacksonParser( json, StatusBody.class );
                }
            default:
                throw unexpectedResponse( verbose, connection, "Trigger import/restore after successful upload" );
            }
        }
        finally
        {
            connection.disconnect();
        }
    }

    /**
     * Asks about how far the upload has gone so far, typically after being interrupted one way or another. The result of this method can be fed into {@link
     * #resumeUpload(boolean, Path, String, long, long, URL, ProgressTrackingOutputStream.Progress)} to resume an upload.
     */
    private long getResumablePosition( boolean verbose, long sourceLength, URL uploadLocation ) throws IOException, CommandFailedException
    {
        debug( verbose, "Asking about resumable position for the upload" );
        HttpURLConnection connection = (HttpURLConnection) uploadLocation.openConnection();
        try
        {
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
        finally
        {
            connection.disconnect();
        }
    }

    private String extractSignedURIFromResponse( boolean verbose, HttpURLConnection connection ) throws IOException, CommandFailedException
    {
        try ( InputStream responseData = connection.getInputStream() )
        {
            String json = new String( toByteArray( responseData ), UTF_8 );
            debug( verbose, "Got json '" + json + "' back expecting to contain the signed URL" );
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
        debugResponse( verbose, connection, false );
    }

    private void debugResponse( boolean verbose, HttpURLConnection connection, boolean successful ) throws IOException
    {
        if ( verbose )
        {
            String responseString = "not available";
            try ( InputStream responseData = successful ? connection.getInputStream() : connection.getErrorStream() )
            {
                responseString = new String( toByteArray( responseData ), UTF_8 );
            }
            debugResponse( true, responseString, connection, successful );
        }
    }

    private void debugResponse( boolean verbose, String responseBody, HttpURLConnection connection, boolean successful )
            throws IOException
    {
        if ( verbose )
        {
            debug( true, "=== Unexpected response ===" );
            debug( true, "Response message: " + connection.getResponseMessage() );
            debug( true, "Response headers:" );
            connection.getHeaderFields().forEach( ( key, value1 ) ->
                                                  {
                                                      for ( String value : value1 )
                                                      {
                                                          debug( true, "  " + key + ": " + value );
                                                      }
                                                  } );
            debug( true, "Error response data: " + responseBody );
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
                                           String.format( "neo4j-admin push-to-cloud --%s=%s --%s=%s", "dump", dump.toFile().getAbsolutePath(), "bolt-uri",
                                                          boltUri ) );
    }

    private CommandFailedException updatePluginErrorResponse( HttpURLConnection connection ) throws IOException
    {
        debugErrorResponse( true, connection );
        return new CommandFailedException(
                "We encountered a problem while communicating to the Neo4j Aura system. " +
                "Please check that you are using the latest version of the push-to-cloud plugin and upgrade if necessary. " +
                "If this problem persists after upgrading, please contact support and attach the logs shown below to your ticket in the support portal." );
    }

    private CommandFailedException validationFailureErrorResponse( boolean verbose, HttpURLConnection connection, long fileSize )
            throws IOException
    {
        try ( InputStream responseData = connection.getErrorStream() )
        {
            String responseString = new String( toByteArray( responseData ), UTF_8 );
            debugResponse( verbose, responseString, connection, false );
            ErrorBody errorBody = parseJsonUsingJacksonParser( responseString, ErrorBody.class );

            String message = errorBody.getMessage();

            switch ( errorBody.getReason() )
            {
            case ERROR_REASON_EXCEEDS_MAX_SIZE:
                String trimmedMessage = StringUtils.removeEnd( message, "." );
                message = format( "%s. Minimum storage space required: %.1f GB", trimmedMessage, bytesToGibibytes( fileSize ) );
                break;
            default:
                break; // No special treatment required
            }

            return formatCommandFailedExceptionError( message, errorBody.getUrl() );
        }
    }

    private CommandFailedException insufficientSpaceErrorResponse( boolean verbose, HttpURLConnection connection, long fileSize )
            throws IOException
    {
        debugErrorResponse( verbose, connection );
        return new CommandFailedException(
                format( "There is insufficient space in your Neo4j Aura instance to upload your data. "
                        + "Please go to the Neo4j Aura Console to increase the size of your database "
                        + "to at least %.1f GB of storage.", bytesToGibibytes( fileSize ) ) );
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

    private double bytesToGibibytes( long sizeInBytes )
    {
        return sizeInBytes / (double) (1024 * 1024 * 1024);
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
