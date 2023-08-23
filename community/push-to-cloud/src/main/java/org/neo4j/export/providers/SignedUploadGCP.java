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

import static java.lang.Long.min;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.compress.utils.IOUtils.toByteArray;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.io.IOUtils;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.CommandResponseHandler;
import org.neo4j.export.UploadCommand;
import org.neo4j.export.util.IOCommon;
import org.neo4j.export.util.ProgressTrackingOutputStream;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class SignedUploadGCP implements SignedUpload {
    static final int HTTP_RESUME_INCOMPLETE = 308;
    private static final long POSITION_UPLOAD_COMPLETED = -1;
    private static final long DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS = SECONDS.toMillis(64);
    private static final long DEFAULT_MAXIMUM_RETRIES = 50;
    private static final String UPLOAD_RESPONSE_ERROR_MESSAGE = "Encountered unexpected response uploading to storage";
    String[] signedLinks;
    String signedURI;
    ExecutionContext ctx;
    ProgressListenerFactory progressListenerFactory =
            (text, length) -> ProgressMonitorFactory.textual(ctx.out()).singlePart(text, length);
    Sleeper sleeper;
    String boltURI;
    private final CommandResponseHandler commandResponseHandler;

    public SignedUploadGCP(
            String[] signedLinks,
            String signedURI,
            ExecutionContext ctx,
            String boltURI,
            CommandResponseHandler commandResponseHandler) {
        this.signedLinks = signedLinks;
        this.signedURI = signedURI;
        this.ctx = ctx;
        this.boltURI = boltURI;
        this.sleeper = Thread::sleep;
        this.commandResponseHandler = commandResponseHandler;
    }

    public SignedUploadGCP(
            String[] signedLinks,
            String signedURI,
            ExecutionContext ctx,
            String boltURI,
            ProgressListenerFactory progressListenerFactory,
            Sleeper sleeper,
            CommandResponseHandler commandResponseHandler) {
        this.signedLinks = signedLinks;
        this.signedURI = signedURI;
        this.ctx = ctx;
        this.boltURI = boltURI;
        this.progressListenerFactory = progressListenerFactory;
        this.sleeper = sleeper;
        this.commandResponseHandler = commandResponseHandler;
    }

    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /**
     * Parses a response from asking about how far an upload has gone, i.e. how many bytes of the source file have been uploaded. The range is in the format:
     * "bytes=x-y" and since we always ask from 0 then we're interested in y, more specifically y+1 since x-y means that bytes in the range x-y have been
     * received so we want to start sending from y+1.
     */
    private static long parseResumablePosition(String range) {
        int dashIndex = range.indexOf('-');
        if (!range.startsWith("bytes=") || dashIndex == -1) {
            throw new CommandFailedException(
                    "Unexpected response when asking where to resume upload from. got '" + range + "'");
        }
        return Long.parseLong(range.substring(dashIndex + 1)) + 1;
    }

    private URL initiateResumableUpload(boolean verbose) throws IOException {

        URL signedURL = getCorrectVersionedEndpoint();
        HttpURLConnection connection = (HttpURLConnection) signedURL.openConnection();
        try (Closeable c = connection::disconnect) {

            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Length", "0");
            connection.setFixedLengthStreamingMode(0);
            connection.setRequestProperty("x-goog-resumable", "start");
            // We don't want to have any Content-Type set really, but there's this issue with the standard
            // HttpURLConnection
            // implementation where it defaults Content-Type to application/x-www-form-urlencoded for POSTs for some
            // reason
            connection.setRequestProperty("Content-Type", "");
            connection.setDoOutput(true);
            int responseCode = connection.getResponseCode();
            switch (responseCode) {
                case HTTP_CREATED:
                    return IOCommon.safeUrl(connection.getHeaderField("Location"));
                case HTTP_GATEWAY_TIMEOUT:
                case HTTP_BAD_GATEWAY:
                case HTTP_UNAVAILABLE:
                    throw new SignedUploadURLFactory.RetryableHttpException(commandResponseHandler.unexpectedResponse(
                            verbose, connection, "Initiating database upload"));
                default:
                    throw commandResponseHandler.unexpectedResponse(verbose, connection, "Initiating database upload");
            }
        }
    }

    @Override
    public void copy(boolean verbose, UploadCommand.Source source) {
        URL dest;
        try {
            dest = initiateResumableUpload(verbose);
        } catch (IOException e) {
            ctx.err().println("Failed to initiate a resumable upload");
            throw new CommandFailedException("Failed to initiate resumable upload", e);
        }
        transfer(verbose, source, dest);
    }

    public URL getCorrectVersionedEndpoint() {
        URL dest;
        // V1
        if (signedLinks != null && signedLinks.length > 0) dest = IOCommon.safeUrl(signedLinks[0]);
        // V2
        else {
            dest = IOCommon.safeUrl(signedURI);
        }
        return dest;
    }

    private void transfer(boolean verbose, UploadCommand.Source source, URL dest) {
        try {
            long sourceLength = ctx.fs().getFileSize(source.path());

            // Enter the resume:able upload loop
            long position = 0;
            int resumeUploadRetries = 0;
            commandResponseHandler.debug(verbose, "copying to URL: " + dest);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            ProgressTrackingOutputStream.Progress uploadProgress = new ProgressTrackingOutputStream.Progress(
                    progressListenerFactory.create("Upload", sourceLength), position);
            while (!resumeUpload(verbose, source.path(), sourceLength, position, dest, uploadProgress)) {
                commandResponseHandler.debug(verbose, "Getting resume position");
                position = getResumablePosition(verbose, sourceLength, dest);
                if (position == POSITION_UPLOAD_COMPLETED) {
                    // This is somewhat unexpected, we didn't get an OK from the upload, but when we asked about how far
                    // the upload
                    // got it responded that it was fully uploaded. I'd guess we're fine here.
                    break;
                }

                // Truncated exponential backoff
                if (resumeUploadRetries > DEFAULT_MAXIMUM_RETRIES) {
                    throw new CommandFailedException("Upload failed after numerous attempts.");
                }
                long backoffFromRetryCount = SECONDS.toMillis(1L << resumeUploadRetries++) + random.nextInt(1_000);
                sleeper.sleep(min(backoffFromRetryCount, DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS));
            }
            ctx.out().println("Upload completed successfully\n");
            uploadProgress.done();

        } catch (InterruptedException | IOException e) {
            ctx.out().println("Failed to upload database" + e.getCause());
            throw new CommandFailedException(e.getMessage(), e);
        }
    }

    private CommandFailedException resumePossibleErrorResponse(HttpURLConnection connection, Path dump)
            throws IOException {
        commandResponseHandler.debugErrorResponse(true, connection);

        return new CommandFailedException("We encountered a problem while communicating to the Neo4j Aura system. \n"
                + "You can re-try using the existing dump by running this command: \n"
                + String.format(
                        "neo4j-admin database upload --%s=%s --%s=%s", "dump", dump.getParent(), "bolt-uri", boltURI));
    }

    /**
     * Uploads source from the given position to the upload location.
     */
    private boolean resumeUpload(
            boolean verbose,
            Path source,
            long sourceLength,
            long position,
            URL uploadLocation,
            ProgressTrackingOutputStream.Progress uploadProgress)
            throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uploadLocation.openConnection();
        try (Closeable c = connection::disconnect) {
            connection.setRequestMethod("PUT");
            long contentLength = sourceLength - position;
            connection.setRequestProperty("Content-Length", String.valueOf(contentLength));
            connection.setFixedLengthStreamingMode(contentLength);
            if (position > 0) {
                // If we're not starting from the beginning we need to specify what range we're uploading in this format
                connection.setRequestProperty(
                        "Content-Range", format("bytes %d-%d/%d", position, sourceLength - 1, sourceLength));
                commandResponseHandler.debug(
                        true,
                        "resume upload from " + position + " to " + (sourceLength - 1) + " of " + sourceLength
                                + " bytes");
            }

            connection.setDoOutput(true);
            uploadProgress.rewindTo(position);
            try (InputStream sourceStream = Files.newInputStream(source);
                    OutputStream targetStream = connection.getOutputStream()) {
                IOCommon.safeSkip(sourceStream, position);
                IOUtils.copy(
                        new BufferedInputStream(sourceStream),
                        new ProgressTrackingOutputStream(targetStream, uploadProgress));
            }
            int responseCode = connection.getResponseCode();
            switch (responseCode) {
                case HTTP_OK:
                    return true; // the file is now uploaded, all good
                case HTTP_FORBIDDEN:
                    if (canSkipToImport(connection.getErrorStream())) {
                        return true;
                    }
                case HTTP_INTERNAL_ERROR:
                case HTTP_UNAVAILABLE:
                case HTTP_BAD_GATEWAY:
                case HTTP_GATEWAY_TIMEOUT:
                    commandResponseHandler.debugErrorResponse(verbose, connection);
                    return false;
                default:
                    commandResponseHandler.debug(true, "resume upload ends\n");
                    throw resumePossibleErrorResponse(connection, source);
            }
        }
    }

    public boolean canSkipToImport(InputStream errorStream) throws IOException {
        String responseString;
        responseString = new String(toByteArray(errorStream), UTF_8);
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

            // Security: Java XML parser has external entities enabled by default.
            // https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html#java
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setXIncludeAware(false);

            DocumentBuilder builder = dbf.newDocumentBuilder();
            Document document = builder.parse(new InputSource(new StringReader(responseString)));
            document.getDocumentElement().normalize();
            Node codeNode = document.getElementsByTagName("Code").item(0);
            Node detailsNode = document.getElementsByTagName("Details").item(0);

            if (handleNullResponse(codeNode, detailsNode)) return false;
            String code = codeNode.getTextContent();
            String details = detailsNode.getTextContent();
            if (handleNullResponse(code, details)) return false;

            String objectExistsText = "does not have storage.objects.delete access to the Google Cloud Storage object.";
            boolean valid = details.contains(objectExistsText) && code.equals("AccessDenied");
            if (!valid) {
                ctx.out().println(UPLOAD_RESPONSE_ERROR_MESSAGE);
                return false;
            } else {
                ctx.out().println("Detected already uploaded object, proceeding to import");
                return true;
            }

        } catch (ParserConfigurationException | SAXException | DOMException e) {
            throw new IOException("Encountered invalid response from cloud import location", e.getCause());
        }
    }

    private boolean handleNullResponse(Object o1, Object o2) {
        if (o1 == null || o2 == null) {
            ctx.out().println(UPLOAD_RESPONSE_ERROR_MESSAGE);
            return true;
        }
        return false;
    }

    /**
     * Asks about how far the upload has gone so far, typically after being interrupted one way or another. The result of this method can be fed into {@link
     * #resumeUpload(boolean, Path, long, long, URL, ProgressTrackingOutputStream.Progress)} to resume an upload.
     */
    private long getResumablePosition(boolean verbose, long sourceLength, URL uploadLocation) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uploadLocation.openConnection();
        try (Closeable c = connection::disconnect) {
            commandResponseHandler.debug(verbose, "Asking about resumable position for the upload");
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Content-Length", "0");
            connection.setFixedLengthStreamingMode(0);
            connection.setRequestProperty("Content-Range", "bytes */" + sourceLength);
            connection.setDoOutput(true);
            int responseCode = connection.getResponseCode();
            switch (responseCode) {
                case HTTP_OK:
                case HTTP_CREATED:
                    commandResponseHandler.debug(verbose, "Upload seems to be completed got " + responseCode);
                    return POSITION_UPLOAD_COMPLETED;
                case HTTP_GATEWAY_TIMEOUT:
                case HTTP_BAD_GATEWAY:
                case HTTP_UNAVAILABLE:
                    throw new SignedUploadURLFactory.RetryableHttpException(commandResponseHandler.unexpectedResponse(
                            verbose, connection, "Acquire resumable upload position"));
                case HTTP_RESUME_INCOMPLETE:
                    String range = connection.getHeaderField("Range");
                    commandResponseHandler.debug(verbose, "Upload not completed got " + range);
                    long position = range == null
                            ? 0 // No bytes have been received at all, so let's return position 0, i.e. from the
                            // beginning of the file
                            : parseResumablePosition(range);
                    commandResponseHandler.debug(verbose, "Parsed that as position " + position);
                    return position;
                default:
                    throw commandResponseHandler.unexpectedResponse(
                            verbose, connection, "Acquire resumable upload position");
            }
        }
    }

    public interface ProgressListenerFactory {
        ProgressListener create(String text, long length);
    }
}
