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
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.compress.utils.IOUtils;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.UploadCommand;
import org.neo4j.export.util.IOCommon;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;

public class SignedUploadAWS implements SignedUpload {

    private static final int RETRIES_COUNT = 5;
    private static final long DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS = SECONDS.toMillis(64);

    private final String[] signedLinks;
    private final String uploadID;

    private final String boltURI;
    private final int totalParts;
    private final IOCommon.Sleeper sleeper;
    private final ExecutionContext ctx;

    public SignedUploadAWS(
            String[] signedLinks, String uploadID, int totalParts, ExecutionContext ctx, String boltURI) {
        this.signedLinks = signedLinks;
        this.uploadID = uploadID;
        this.totalParts = totalParts;
        this.ctx = ctx;
        this.boltURI = boltURI;
        this.sleeper = Thread::sleep;
    }

    public SignedUploadAWS(
            String[] signedLinks,
            String uploadID,
            int totalParts,
            ExecutionContext ctx,
            String boltURI,
            IOCommon.Sleeper sleeper) {
        this.signedLinks = signedLinks;
        this.uploadID = uploadID;
        this.totalParts = totalParts;
        this.ctx = ctx;
        this.boltURI = boltURI;
        this.sleeper = sleeper;
    }

    @Override
    public void copy(boolean verbose, UploadCommand.Source src) {
        try {
            upload(src);
        } catch (IOException e) {
            throw new CommandFailedException(e.getMessage(), e);
        }
    }

    public void upload(UploadCommand.Source src) throws IOException {
        long fileSize;
        fileSize = IOCommon.getFileSize(src, ctx);
        ProgressListener progressListener = ProgressMonitorFactory.textual(ctx.out())
                .singlePart(
                        "Uploading to AWS (This may take a some time depending on file size and connection speed)",
                        signedLinks.length);
        long totalBytesCopied = copyToUrls(src, progressListener, fileSize);
        progressListener.close();
        ctx.out().println("Total bytes copied: " + totalBytesCopied);
    }

    private long copyToUrls(UploadCommand.Source src, ProgressListener progressListener, long fileSize)
            throws IOException {

        Path source = src.path();
        long totalBytesCopied = 0;
        int uploadPartPosition = getUploadPartPosition();
        for (int i = 0; i < signedLinks.length; i++) {
            totalBytesCopied += copyToUrl(i, uploadPartPosition, signedLinks[i], source, progressListener, fileSize);
            uploadPartPosition++;
        }
        return totalBytesCopied;
    }

    private long copyToUrl(
            int currentPositionInArray,
            int uploadPartPosition,
            String link,
            Path source,
            ProgressListener progressListener,
            long filesize)
            throws IOException {
        int retries = 0;
        while (retries < RETRIES_COUNT) {
            URL url = IOCommon.safeUrl(link);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            try (Closeable ignored = connection::disconnect) {
                connection.setRequestMethod("PUT");
                connection.setDoOutput(true);

                long chunkSize = getChunkSize(filesize);
                Path sourcePath = source.toAbsolutePath();
                long skipAmount = uploadPartPosition * chunkSize;

                if (currentPositionInArray == signedLinks.length - 1) {
                    // We need chunkSize and last chunkSize
                    chunkSize = filesize - ((totalParts - 1) * chunkSize);
                }

                connection.setFixedLengthStreamingMode(chunkSize);

                try (InputStream sourceStream = new BufferedInputStream(Files.newInputStream(sourcePath))) {
                    IOCommon.safeSkip(sourceStream, skipAmount);
                    long bytesCopied =
                            copyToMultiPartURL(currentPositionInArray, sourceStream, chunkSize, connection, filesize);
                    checkResponseOk(connection, source);
                    progressListener.add(1);
                    return bytesCopied;
                } catch (RetryableHttpException e) {
                    ctx.err()
                            .println(String.format(
                                    "%s Failed to upload part %d to multipart url. Retrying in case of connection issue",
                                    e.getMessage(), currentPositionInArray));

                    waitBeforeNextAttempt(retries);
                    retries++;
                }
            }
        }
        throw new CommandFailedException("Failed to upload part to multipart url after " + RETRIES_COUNT
                + " retries. Please check your Internet connection and try again.");
    }

    private void waitBeforeNextAttempt(int attemptNumber) {
        try {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            long backoffFromRetryCount = SECONDS.toMillis(1L << attemptNumber) + random.nextInt(1_000);
            sleeper.sleep(min(backoffFromRetryCount, DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS));
        } catch (InterruptedException e) {
            ctx.err().println("Interrupted waiting to retry connection to upload URL");
            throw new CommandFailedException(e.getMessage());
        }
    }

    private void checkResponseOk(HttpURLConnection connection, Path dumpDir) throws RetryableHttpException {
        int responseCode = 0;
        try {
            responseCode = connection.getResponseCode();

            switch (responseCode) {
                case HTTP_OK:
                    return; // the part is uploaded, all good
                case HTTP_INTERNAL_ERROR:
                case HTTP_UNAVAILABLE:
                case HTTP_BAD_GATEWAY:
                case HTTP_GATEWAY_TIMEOUT:
                    throw new RetryableHttpException(new IOException("Received error response from server"));
                default:
                    ctx.err().println(String.format("Received HTTP error: %d uploading to AWS", responseCode));
                    throw resumePossibleErrorResponse(dumpDir);
            }
        } catch (IOException e) {
            ctx.out().println("Response code: " + responseCode);
            throw new RetryableHttpException(e);
        }
    }

    private CommandFailedException resumePossibleErrorResponse(Path dump) throws IOException {
        return new CommandFailedException("We encountered a problem while communicating to the Neo4j Aura system. \n"
                + "You can re-try using the existing dump by running this command: \n"
                + String.format(
                        "neo4j-admin push-to-cloud --%s=%s --%s=%s",
                        "dump", dump.toAbsolutePath(), "bolt-uri", boltURI));
    }

    private long copyToMultiPartURL(
            int currentPosition, InputStream sourceStream, long chunkSize, HttpURLConnection connection, long fileSize)
            throws RetryableHttpException {
        try {

            try (OutputStream outputStream = connection.getOutputStream()) {

                if (currentPosition == signedLinks.length - 1) {
                    // for the last item we want to copy everything left over not just chunkSize
                    return IOUtils.copy(sourceStream, connection.getOutputStream());
                }
                return IOUtils.copyRange(sourceStream, chunkSize, outputStream);
            }

        } catch (IOException e) {
            ctx.err().println(e.getMessage());
            throw new RetryableHttpException(e);
        }
    }

    static class RetryableHttpException extends RuntimeException {
        RetryableHttpException(IOException e) {
            super(e);
        }
    }

    public long getChunkSize(long fileSize) {
        if (totalParts <= 1) {
            return fileSize;
        }
        return (long) Math.ceil(fileSize / (double) totalParts);
    }

    private int getUploadPartPosition() {
        return totalParts - signedLinks.length;
    }
}
