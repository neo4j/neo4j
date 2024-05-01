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
package org.neo4j.export.aura;

import static java.lang.Long.min;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.compress.utils.IOUtils.toByteArray;
import static org.neo4j.export.UploadCommand.bytesToGibibytes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.export.CommandResponseHandler;
import org.neo4j.export.aura.AuraJsonMapper.ErrorBody;
import org.neo4j.export.aura.AuraJsonMapper.SignedURIBodyResponse;
import org.neo4j.export.aura.AuraJsonMapper.StatusBody;
import org.neo4j.export.aura.AuraJsonMapper.UploadStatusResponse;
import org.neo4j.export.util.IOCommon;
import org.neo4j.export.util.ProgressTrackingOutputStream;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.time.Clocks;

/* Handles requests to the console without interacting with pre-signed URLs*/
public class AuraClient {

    static final int HTTP_UNPROCESSABLE_ENTITY = 422;
    static final String ERROR_REASON_EXCEEDS_MAX_SIZE = "ImportExceedsMaxSize";

    static final int HTTP_TOO_MANY_REQUESTS = 429;

    private static final long DEFAULT_MAXIMUM_RETRIES = 50;
    private static final long DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS = SECONDS.toMillis(64);
    private final AuraConsole auraConsole;
    private final String username;
    private final char[] password;
    private final boolean consentConfirmed;
    private final String boltURI;
    private final Clock clock;
    private final ProgressListenerFactory progressListenerFactory;
    private final CommandResponseHandler commandResponseHandler;
    private final ExecutionContext ctx;
    private final IOCommon.Sleeper sleeper;
    private boolean verbose;

    public AuraClient(AuraClientBuilder auraClientBuilder) {
        this.ctx = auraClientBuilder.ctx;
        this.auraConsole = auraClientBuilder.auraConsole;
        this.username = auraClientBuilder.username;
        this.password = auraClientBuilder.password;
        this.consentConfirmed = auraClientBuilder.consentConfirmed;
        this.boltURI = auraClientBuilder.boltURI;
        this.sleeper = auraClientBuilder.sleeper;
        this.clock = auraClientBuilder.clock;
        this.progressListenerFactory = auraClientBuilder.progressListenerFactory;
        this.commandResponseHandler = auraClientBuilder.commandResponseHandler;
    }

    public AuraConsole getAuraConsole() {
        return auraConsole;
    }

    private String getClientVersion() {
        return getClass().getPackage().getImplementationVersion();
    }

    public String authenticate(boolean verbose) throws CommandFailedException {
        try {
            return doAuthenticate(verbose);
        } catch (IOException e) {
            ctx.err().println("Failed to authenticate with the aura console");
            throw new CommandFailedException("Failed to authenticate", e);
        }
    }

    /**
     * Communication with Neo4j's cloud console, resulting in some signed URI to do the actual upload to.
     */
    public SignedURIBodyResponse initatePresignedUpload(
            long crc32Sum, long dumpSize, long fullStoreSize, String bearerToken) {
        URL importURL = auraConsole.getImportUrl();
        return retryOnUnavailable(
                () -> doInitatePresignedUpload(crc32Sum, dumpSize, fullStoreSize, bearerToken, importURL));
    }

    private SignedURIBodyResponse doInitatePresignedUpload(
            long crc32Sum, long dumpSize, long fullSize, String bearerToken, URL importURL) throws IOException {

        HttpURLConnection connection = (HttpURLConnection) importURL.openConnection();
        String bearerHeader = "Bearer " + bearerToken;
        try (Closeable c = connection::disconnect) {
            // POST the request
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", bearerHeader);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Neo4j-Version", getClientVersion());
            connection.setDoOutput(true);
            try (OutputStream postData = connection.getOutputStream()) {
                postData.write(
                        String.format("{\"Crc32\":%d, \"DumpSize\": %d, \"FullSize\":%d}", crc32Sum, dumpSize, fullSize)
                                .getBytes(UTF_8));
            }

            // Read the response
            int responseCode = connection.getResponseCode();

            return switch (responseCode) {
                    // fallthrough
                case HTTP_NOT_FOUND, HTTP_MOVED_PERM -> throw updatePluginErrorResponse(connection);
                case HTTP_UNAUTHORIZED -> throw errorResponse(
                        verbose, connection, "The given authorization token is invalid or has expired");
                case HTTP_UNPROCESSABLE_ENTITY -> throw validationFailureErrorResponse(connection, fullSize);
                case HTTP_GATEWAY_TIMEOUT, HTTP_BAD_GATEWAY, HTTP_UNAVAILABLE -> throw new RetryableHttpException(
                        commandResponseHandler.unexpectedResponse(verbose, connection, "Initiating upload target"));
                case HTTP_ACCEPTED ->
                // the import request was accepted, and the server has not seen this dump file, meaning the import
                // request is a new operation.
                extractSignedURIFromResponse(connection);
                default -> throw commandResponseHandler.unexpectedResponse(
                        verbose, connection, "Initiating upload target");
            };
        }
    }

    private SignedURIBodyResponse extractSignedURIFromResponse(HttpURLConnection connection) throws IOException {
        try (InputStream responseData = connection.getInputStream()) {
            String json = new String(toByteArray(responseData), UTF_8);
            return IOCommon.parseJsonUsingJacksonParser(json, AuraJsonMapper.SignedURIBodyResponse.class);
        }
    }

    private String doAuthenticate(boolean verbose) throws IOException {
        URL url = auraConsole.getAuthenticateUrl();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try (Closeable c = connection::disconnect) {
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", "Basic " + IOCommon.base64Encode(username, password));
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Confirmed", String.valueOf(consentConfirmed));
            int responseCode = connection.getResponseCode();
            switch (responseCode) {
                case HTTP_NOT_FOUND -> throw errorResponse(
                        verbose,
                        connection,
                        "We encountered a problem while contacting your Neo4j Aura instance, "
                                + "please check your Bolt URI");
                case HTTP_MOVED_PERM -> throw updatePluginErrorResponse(connection);
                case HTTP_UNAUTHORIZED -> throw errorResponse(
                        verbose, connection, "Invalid username/password credentials");
                case HTTP_FORBIDDEN -> throw errorResponse(
                        verbose,
                        connection,
                        "The credentials provided do not give administrative access to the target database");
                case HTTP_CONFLICT -> throw errorResponse(
                        verbose, connection, "No consent to overwrite database. Aborting");
                case HTTP_GATEWAY_TIMEOUT, HTTP_BAD_GATEWAY, HTTP_UNAVAILABLE -> throw new RetryableHttpException(
                        commandResponseHandler.unexpectedResponse(verbose, connection, "Authorization"));
                case HTTP_OK -> {
                    try (InputStream responseData = connection.getInputStream()) {
                        String json = new String(toByteArray(responseData), UTF_8);
                        commandResponseHandler.debug(true, "Successfully authenticated with Aura.");
                        return IOCommon.parseJsonUsingJacksonParser(json, TokenBody.class).Token;
                    }
                }
                default -> throw commandResponseHandler.unexpectedResponse(verbose, connection, "Authorization");
            }
        }
    }

    static class RetryableHttpException extends RuntimeException {
        RetryableHttpException(CommandFailedException e) {
            super(e);
        }
    }

    <T> T retryOnUnavailable(IOExceptionSupplier<T> runnableCommand) {
        int attempt = 0;
        RetryableHttpException lastException = null;
        while (true) {
            try {
                return runnableCommand.get();
            } catch (RetryableHttpException e) {
                if (attempt >= DEFAULT_MAXIMUM_RETRIES) // Will retry one more, so in the end we have 1 + (n+1) retries
                {
                    break;
                }
                // Truncated exponential backoff
                ThreadLocalRandom random = ThreadLocalRandom.current();
                long backoffFromRetryCount = SECONDS.toMillis(1L << attempt++) + random.nextInt(1_000);
                try {
                    sleeper.sleep(min(backoffFromRetryCount, DEFAULT_MAXIMUM_RETRY_BACKOFF_MILLIS));
                } catch (InterruptedException ex) {
                    throw new CommandFailedException(e.getMessage(), e);
                }
                lastException = e;
            } catch (IOException e) {
                throw new CommandFailedException(e.getMessage(), e);
            }
        }

        throw (RuntimeException) lastException.getCause();
    }

    public void checkSize(boolean verbose, long size, String bearerToken) {
        retryOnUnavailable(() -> {
            doCheckSize(verbose, size, bearerToken);
            return null;
        });
    }

    private void doCheckSize(boolean verbose, long size, String bearerToken) throws IOException {
        URL url = auraConsole.getSizeUrl();
        String bearerTokenHeader = "Bearer " + bearerToken;
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try (Closeable c = connection::disconnect) {
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", bearerTokenHeader);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            try (OutputStream postData = connection.getOutputStream()) {
                postData.write(String.format("{\"FullSize\":%d}", size).getBytes(UTF_8));
            }
            int responseCode = connection.getResponseCode();
            switch (responseCode) {
                case HTTP_UNPROCESSABLE_ENTITY -> throw validationFailureErrorResponse(connection, size);
                case HTTP_OK -> {}
                case HTTP_GATEWAY_TIMEOUT, HTTP_BAD_GATEWAY, HTTP_UNAVAILABLE -> throw new RetryableHttpException(
                        commandResponseHandler.unexpectedResponse(verbose, connection, "Size check"));
                default -> throw commandResponseHandler.unexpectedResponse(verbose, connection, "Size check");
            }
        }
    }

    public void doStatusPolling(boolean verbose, String bearerToken, long fileSize) throws InterruptedException {
        ctx.out().println("We have received your export and it is currently being loaded into your Aura instance.");
        ctx.out()
                .println(
                        "You can wait here, or abort this command and head over to the console to be notified of when your database is running.");
        String bearerTokenHeader = "Bearer " + bearerToken;
        ProgressTrackingOutputStream.Progress statusProgress = new ProgressTrackingOutputStream.Progress(
                progressListenerFactory.create("Import progress (estimated)", 100L), 0);
        boolean importHasStarted = false;
        long importStarted = this.clock.millis();
        double importTimeEstimateMinutes = 5 + (3 * bytesToGibibytes(fileSize));
        long importTimeEstimateMillis = SECONDS.toMillis((long) (importTimeEstimateMinutes * 60));
        long importStartedTimeout =
                importStarted + 90 * 1000; // timeout to switch from first running to loading = 1.5 minute
        commandResponseHandler.debug(
                verbose,
                format(
                        "Rough guess for how long dump file import will take: %.0f minutes; store size on disk is %.1f GB (%d bytes)",
                        importTimeEstimateMinutes, bytesToGibibytes(fileSize), fileSize));
        while (!statusProgress.isDone()) {
            StatusBody statusBody = getDatabaseStatus(verbose, auraConsole.getStatusUrl(), bearerTokenHeader);
            switch (statusBody.Status) {
                case "running" -> {
                    // It could happen that the very first call of this method is so fast, that the database is still in
                    // state
                    // "running". So we need to check if this is the case and ignore the result in that case and only
                    // take this result as valid, once the status loading or restoring was seen before.
                    if (importHasStarted) {
                        statusProgress.rewindTo(0);
                        statusProgress.add(100);
                        statusProgress.done();
                    } else {
                        throwIfImportDidNotStart(importStartedTimeout);
                    }
                }
                case "loading failed" -> {
                    if (importHasStarted) {
                        throw formatCommandFailedExceptionError(
                                statusBody.Error.getMessage(), statusBody.Error.getUrl());
                    } else {
                        throwIfImportDidNotStart(importStartedTimeout);
                    }
                }
                default -> {
                    importHasStarted = true;
                    long elapsed = this.clock.millis() - importStarted;
                    statusProgress.rewindTo(0);
                    statusProgress.add(
                            importStatusProgressEstimate(statusBody.Status, elapsed, importTimeEstimateMillis));
                }
            }
            sleeper.sleep(2000);
        }
        ctx.out().println("Your data was successfully pushed to Aura and is now running.");
        long importDurationMillis = this.clock.millis() - importStarted;
        commandResponseHandler.debug(
                verbose,
                format(
                        "Import took about %d minutes to complete excluding upload (%d ms)",
                        MILLISECONDS.toMinutes(importDurationMillis), importDurationMillis));
    }

    private CommandFailedException formatCommandFailedExceptionError(String message, String url) {
        if (StringUtils.isEmpty(url)) {
            return new CommandFailedException(message);
        } else {
            String trimmedMessage = StringUtils.removeEnd(message, ".");
            return new CommandFailedException(format("Error: %s. See: %s", trimmedMessage, url));
        }
    }

    public int importStatusProgressEstimate(String databaseStatus, long elapsed, long importTimeEstimateMillis) {
        switch (databaseStatus) {
            case "running" -> {
                return 0;
            }
            case "updating", "loading" -> {
                int loadProgressEstimation = (int) Math.min(98, (elapsed * 98) / importTimeEstimateMillis);
                return 1 + loadProgressEstimation;
            }
            default -> throw new CommandFailedException(String.format(
                    "We're sorry, something has failed during the loading of your database. "
                            + "Please try again and if this problem persists, please open up a support case. Database status: %s",
                    databaseStatus));
        }
    }

    private StatusBody doGetDatabaseStatus(boolean verbose, URL statusURL, String bearerToken) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) statusURL.openConnection();
        try (Closeable c = connection::disconnect) {
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", bearerToken);
            connection.setDoOutput(true);

            int responseCode = connection.getResponseCode();
            switch (responseCode) {
                    // fallthrough
                case HTTP_NOT_FOUND, HTTP_MOVED_PERM -> throw updatePluginErrorResponse(connection);
                case HTTP_OK -> {
                    try (InputStream responseData = connection.getInputStream()) {
                        String json = new String(toByteArray(responseData), UTF_8);
                        // debugResponse( verbose, json, connection, false );
                        return IOCommon.parseJsonUsingJacksonParser(json, StatusBody.class);
                    }
                }
                case HTTP_GATEWAY_TIMEOUT, HTTP_INTERNAL_ERROR, HTTP_BAD_GATEWAY, HTTP_UNAVAILABLE -> {
                    ctx.err().println("Received HTTP 5xx error polling status. Retrying...");
                    throw new RetryableHttpException(commandResponseHandler.unexpectedResponse(
                            verbose, connection, "Trigger import/restore after successful upload"));
                }
                default -> throw commandResponseHandler.unexpectedResponse(
                        verbose, connection, "Trigger import/restore after successful upload");
            }
        }
    }

    public UploadStatusResponse uploadStatus(boolean verbose, long crc32Sum, String uploadId, String bearerToken)
            throws IOException {
        URL uploadStatusUrl = auraConsole.getUploadStatusUrl();
        HttpURLConnection connection = (HttpURLConnection) uploadStatusUrl.openConnection();
        String bearerHeader = "Bearer " + bearerToken;

        try (Closeable c = connection::disconnect; ) {
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", bearerHeader);
            connection.setDoOutput(true);
        }
        try (OutputStream postData = connection.getOutputStream()) {
            postData.write(String.format("{\"Crc32\":%d, \"UploadID\": \"%s\"}", crc32Sum, uploadId)
                    .getBytes(UTF_8));
        }

        try (InputStream responseData = connection.getInputStream()) {
            String json = new String(toByteArray(responseData), UTF_8);
            // debugResponse( verbose, json, connection, false );

            return IOCommon.parseJsonUsingJacksonParser(json, UploadStatusResponse.class);
        }
    }

    public void triggerAWSImportProtocol(
            boolean verbose, Path source, long crc32Sum, String bearerToken, UploadStatusResponse uploadStatusResponse)
            throws IOException {

        URL completeImportURL = auraConsole.getUploadCompleteUrl();
        HttpURLConnection connection = (HttpURLConnection) completeImportURL.openConnection();
        String bearerHeader = "Bearer " + bearerToken;
        try (Closeable c = connection::disconnect) {
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", bearerHeader);
            connection.setRequestProperty("Neo4j-Version", getClientVersion());
            connection.setDoOutput(true);
            try (OutputStream postData = connection.getOutputStream()) {

                AuraJsonMapper.TriggerImportRequest triggerImportRequest = new AuraJsonMapper.TriggerImportRequest();
                triggerImportRequest.uploadStatusResponse = uploadStatusResponse;
                triggerImportRequest.Crc32 = crc32Sum;
                String json = IOCommon.SerializeWithJackson(triggerImportRequest);
                postData.write(json.getBytes(UTF_8));
            }

            checkTriggerImportResponseCode(verbose, source, connection);
        }
    }

    public void triggerGCPImportProtocol(boolean verbose, Path source, long crc32Sum, String bearerToken)
            throws IOException {

        URL completeImportURL = auraConsole.getUploadCompleteUrl();
        HttpURLConnection connection = (HttpURLConnection) completeImportURL.openConnection();
        String bearerHeader = "Bearer " + bearerToken;
        try (Closeable c = connection::disconnect) {
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", bearerHeader);
            connection.setRequestProperty("Neo4j-Version", getClientVersion());
            connection.setDoOutput(true);
            try (OutputStream postData = connection.getOutputStream()) {
                postData.write(String.format("{\"Crc32\":%d}", crc32Sum).getBytes(UTF_8));
            }

            checkTriggerImportResponseCode(verbose, source, connection);
        }
    }

    private void checkTriggerImportResponseCode(boolean verbose, Path source, HttpURLConnection connection)
            throws IOException {
        int responseCode = connection.getResponseCode();

        switch (responseCode) {
            case HTTP_NOT_FOUND:
                // fallthrough
            case HTTP_MOVED_PERM:
                throw updatePluginErrorResponse(connection);
            case HTTP_TOO_MANY_REQUESTS:
                throw resumePossibleErrorResponse(connection, source);
            case HTTP_CONFLICT:
                throw errorResponse(
                        verbose,
                        connection,
                        "The target database contained data and consent to overwrite the data was not given. Aborting");
            case HTTP_OK:
                // All good, we managed to trigger the import protocol after our completed upload
                break;
            default:
                throw resumePossibleErrorResponse(connection, source);
        }
    }

    private void throwIfImportDidNotStart(long importStartedTimeout) {
        boolean passedStartImportTimeout = this.clock.millis() > importStartedTimeout;
        if (passedStartImportTimeout) {
            throw new CommandFailedException(
                    "Timed out waiting for database load to start as the database did not enter "
                            + "'loading' state in time. Please retry the operation. You might find more information about the "
                            + "failure on the database status page in https://console.neo4j.io.");
        }
    }

    private StatusBody getDatabaseStatus(boolean verbose, URL statusURL, String bearerToken) {
        return retryOnUnavailable(() -> doGetDatabaseStatus(verbose, statusURL, bearerToken));
    }

    private CommandFailedException resumePossibleErrorResponse(HttpURLConnection connection, Path dump)
            throws IOException {
        commandResponseHandler.debugErrorResponse(true, connection);

        return new CommandFailedException("We encountered a problem while communicating to the Neo4j Aura system. \n"
                + "You can re-try using the existing dump by running this command: \n"
                + String.format(
                        "neo4j-admin database upload --%s=%s --%s=%s neo4j",
                        "from-path", dump.getParent().toAbsolutePath(), "to-uri", boltURI));
    }

    private CommandFailedException errorResponse(boolean verbose, HttpURLConnection connection, String errorDescription)
            throws IOException {
        commandResponseHandler.debugErrorResponse(verbose, connection);
        return new CommandFailedException(errorDescription);
    }

    private CommandFailedException updatePluginErrorResponse(HttpURLConnection connection) throws IOException {
        commandResponseHandler.debugErrorResponse(true, connection);
        return new CommandFailedException("We encountered a problem while communicating to the Neo4j Aura system. "
                + "Please check that you are using the latest version of neo4j-admin database upload and upgrade if necessary. "
                + "If this problem persists after upgrading, please contact support at https://support.neo4j.com and attach "
                + "the logs shown below to your ticket in the support portal.");
    }

    private CommandFailedException validationFailureErrorResponse(HttpURLConnection connection, long size)
            throws IOException {
        try (InputStream responseData = connection.getErrorStream()) {
            String responseString = new String(toByteArray(responseData), UTF_8);
            commandResponseHandler.debugResponse(responseString, connection, true);
            ErrorBody errorBody = IOCommon.parseJsonUsingJacksonParser(responseString, ErrorBody.class);

            String message = errorBody.getMessage();

            // No special treatment required
            if (ERROR_REASON_EXCEEDS_MAX_SIZE.equals(errorBody.getReason())) {
                String trimmedMessage = StringUtils.removeEnd(message, ".");
                message = String.format(
                        "%s. Minimum storage space required: %s",
                        trimmedMessage, org.neo4j.export.UploadCommand.sizeText(size));
            }

            return formatCommandFailedExceptionError(message, errorBody.getUrl());
        }
    }

    public interface ProgressListenerFactory {
        ProgressListener create(String text, long length);
    }

    private interface IOExceptionSupplier<T> {
        T get() throws IOException;
    }

    public static class AuraClientBuilder {
        ExecutionContext ctx;
        private AuraConsole auraConsole;

        private String username;

        private char[] password;

        private boolean consentConfirmed;

        private String boltURI;

        private Clock clock;

        private IOCommon.Sleeper sleeper;

        private ProgressListenerFactory progressListenerFactory;

        private CommandResponseHandler commandResponseHandler;

        public AuraClientBuilder(ExecutionContext ctx) {
            this.ctx = ctx;
        }

        public AuraClientBuilder withAuraConsole(AuraConsole auraConsole) {
            this.auraConsole = auraConsole;
            return this;
        }

        public AuraClientBuilder withUserName(String username) {
            this.username = username;
            return this;
        }

        public AuraClientBuilder withPassword(char[] password) {
            this.password = password;
            return this;
        }

        public AuraClientBuilder withConsent(boolean consentConfirmed) {
            this.consentConfirmed = consentConfirmed;
            return this;
        }

        public AuraClientBuilder withBoltURI(String boltURI) {
            this.boltURI = boltURI;
            return this;
        }

        public AuraClientBuilder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public AuraClientBuilder withSleeper(IOCommon.Sleeper sleeper) {
            this.sleeper = sleeper;
            return this;
        }

        public AuraClientBuilder withCommandResponseHandler(CommandResponseHandler commandResponseHandler) {
            this.commandResponseHandler = commandResponseHandler;
            return this;
        }

        public AuraClientBuilder withProgressListenerFactory(ProgressListenerFactory progressListenerFactory) {
            this.progressListenerFactory = progressListenerFactory;
            return this;
        }

        public AuraClientBuilder withDefaults() {
            if (this.sleeper == null) {
                this.sleeper = Thread::sleep;
            }
            if (this.clock == null) {
                this.clock = Clocks.nanoClock();
            }
            this.commandResponseHandler = new CommandResponseHandler(ctx);
            this.progressListenerFactory =
                    (text, length) -> ProgressMonitorFactory.textual(ctx.out()).singlePart(text, length);
            return this;
        }

        public AuraClient build() {
            return new AuraClient(this);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class TokenBody {
        public String Token;
    }
}
