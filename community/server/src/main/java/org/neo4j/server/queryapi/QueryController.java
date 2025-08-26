/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.queryapi;

import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionAccessedConcurrently;
import static org.neo4j.server.queryapi.request.AccessMode.toDriverAccessMode;
import static org.neo4j.server.queryapi.response.HttpErrorResponse.fromDriverException;
import static org.neo4j.server.queryapi.response.HttpErrorResponse.singleError;
import static org.neo4j.server.queryapi.response.QueryResponseBookmarks.fromBookmarks;
import static org.neo4j.server.queryapi.response.QueryResponseTxInfo.fromQueryAPITransaction;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ConcurrentModificationException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.server.queryapi.request.AutoCommitResultContainer;
import org.neo4j.server.queryapi.request.QueryRequest;
import org.neo4j.server.queryapi.request.TxManagedResultContainer;
import org.neo4j.server.queryapi.tx.Transaction;
import org.neo4j.server.queryapi.tx.TransactionConcurrentAccessException;
import org.neo4j.server.queryapi.tx.TransactionIdCollisionException;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.server.queryapi.tx.TransactionNotFoundException;
import org.neo4j.server.queryapi.tx.WrongUserException;
import org.neo4j.server.rest.dbms.AuthorizationHeaders;

public class QueryController {

    private final Driver driver;
    private final InternalLog log;
    private final Duration defaultTimeout;
    private final TransactionManager transactionManager;
    private final Integer txIdLength;

    public QueryController(
            Driver driver,
            InternalLogProvider logProvider,
            Duration defaultTimeout,
            TransactionManager transactionManager,
            Integer txIdLength) {
        this.transactionManager = transactionManager;
        this.driver = driver;
        this.defaultTimeout = defaultTimeout;
        this.txIdLength = txIdLength;
        this.log = logProvider.getLog(QueryController.class);
    }

    public Response executeQuery(QueryRequest request, HttpServletRequest rawRequest, String databaseName) {
        var sessionConfig = buildSessionConfig(request, databaseName);
        // The session will be closed after the result set has been serialized, it must not be closed in a
        // try-with-resources block here. It must be closed only in an exceptional state
        var sessionAuthToken = extractAuthToken(rawRequest);
        if (sessionAuthToken == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Session session = driver.session(Session.class, sessionConfig, sessionAuthToken);

        try {
            var result = session.run(request.statement(), request.parameters());
            var resultContainer = new AutoCommitResultContainer(result, session, request);
            return Response.accepted(resultContainer).build();
        } catch (FatalDiscoveryException ex) {
            return notFoundDiscoveryResponse(ex);
        } catch (ClientException | TransientException clientException) {
            return clientError(clientException);
        } catch (Exception exception) {
            log.error("Local driver failed to execute query", exception);
            return serverError();
        }
    }

    public Response beginTransaction(QueryRequest request, HttpServletRequest rawRequest, String databaseName) {
        var sessionConfig = buildSessionConfig(request, databaseName);
        var txId = randomTxId(txIdLength);
        var sessionAuthToken = extractAuthToken(rawRequest);
        if (sessionAuthToken == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        Session session = driver.session(Session.class, sessionConfig, sessionAuthToken);
        var txCleanUpAction = TxHandling.CLOSE;

        try {
            var queryTransaction = transactionManager.begin(
                    txId, session, sessionAuthToken, databaseName, buildTxConfig(request), request.txType());
            if (request.statement() != null && !request.statement().isEmpty()) {
                queryTransaction.runQuery(request.statement(), request.parameters());
                txCleanUpAction = TxHandling.KEEP_OPEN;
                return successWithResultResponse(queryTransaction, request.includeCounters(), false);
            } else {
                // todo for extra peace of mind, we can release after the writer has done
                txCleanUpAction = TxHandling.RETURN;
                return transactionInfoOnlyResponse(queryTransaction);
            }
        } catch (FatalDiscoveryException ex) {
            return notFoundDiscoveryResponse(ex);
        } catch (TransactionIdCollisionException ignored) {
            return txCollisionResponse();
        } catch (ClientException | TransientException clientException) {
            return clientError(clientException);
        } catch (DatabaseException databaseException) {
            return databaseError(databaseException);
        } catch (Exception exception) {
            log.error("Local driver failed to execute query", exception);
            return serverError();
        } finally {
            cleanUp(txId, txCleanUpAction);
        }
    }

    public Response continueTransaction(
            QueryRequest request, HttpServletRequest rawRequest, String databaseName, String txId) {
        return executeStatement(request, txId, extractAuthToken(rawRequest), databaseName, false);
    }

    public Response commitTransaction(
            QueryRequest request, HttpServletRequest rawRequest, String databaseName, String txId) {
        return executeStatement(request, txId, extractAuthToken(rawRequest), databaseName, true);
    }

    public Response rollbackTransaction(String txId, HttpServletRequest rawRequest, String requestDatabase) {
        Transaction transaction;
        try {
            transaction = transactionManager.retrieveTransaction(txId, requestDatabase, extractAuthToken(rawRequest));
        } catch (TransactionNotFoundException | WrongUserException ex) {
            return transactionNotFoundResponse(txId);
        } catch (ConcurrentModificationException ex) {
            return txConcurrentAccessResponse();
        }

        try {
            transaction.rollback(); // todo handle rollback failure
        } finally {
            transactionManager.removeTransaction(txId);
        }
        return Response.ok().build();
    }

    private Response executeStatement(
            QueryRequest request,
            String txId,
            AuthToken requestAuthToken,
            String requestDatabase,
            boolean requiresCommit) {
        Transaction queryAPITransaction;

        try {
            queryAPITransaction = transactionManager.retrieveTransaction(txId, requestDatabase, requestAuthToken);
        } catch (TransactionNotFoundException | WrongUserException ex) {
            return transactionNotFoundResponse(txId);
        } catch (TransactionConcurrentAccessException ex) {
            return txConcurrentAccessResponse();
        }

        var txCleanUpAction = TxHandling.CLOSE;

        try {
            if (request.statement() != null) {
                queryAPITransaction.runQuery(request.statement(), request.parameters());
                txCleanUpAction = TxHandling.KEEP_OPEN;
                return successWithResultResponse(queryAPITransaction, request.includeCounters(), requiresCommit);
            } else {
                if (requiresCommit) {
                    var bookmarks = queryAPITransaction.commit();
                    return bookmarksOnlyResponse(bookmarks);
                } else {
                    queryAPITransaction.extendTimeout();
                    txCleanUpAction = TxHandling.RETURN;
                    return transactionInfoOnlyResponse(queryAPITransaction);
                }
            }
        } catch (ClientException | TransientException clientException) {
            return clientError(clientException);
        } catch (Exception exception) {
            log.error("Local driver failed to execute query", exception);
            return serverError();
        } finally {
            cleanUp(txId, txCleanUpAction);
        }
    }

    void cleanUp(String txId, TxHandling action) {
        switch (action) {
            case CLOSE:
                transactionManager.removeTransaction(txId);
            case RETURN:
                transactionManager.releaseTransaction(txId);
        }
    }

    public void closeDriver() {
        this.driver.close();
    }

    private SessionConfig buildSessionConfig(QueryRequest request, String databaseName) {
        var sessionConfigBuilder = SessionConfig.builder().withDatabase(databaseName);

        if (!(request.bookmarks() == null || request.bookmarks().isEmpty())) {
            sessionConfigBuilder.withBookmarks(
                    request.bookmarks().stream().map(Bookmark::from).collect(Collectors.toList()));
        }

        if (!(request.impersonatedUser() == null || request.impersonatedUser().isBlank())) {
            sessionConfigBuilder.withImpersonatedUser(request.impersonatedUser().trim());
        }

        if (request.accessMode() != null) {
            sessionConfigBuilder.withDefaultAccessMode(toDriverAccessMode(request.accessMode()));
        }

        return sessionConfigBuilder.build();
    }

    private TransactionConfig buildTxConfig(QueryRequest request) {
        var txConfigBuilder = TransactionConfig.builder();
        if (request.maxExecutionTime() > 0) {
            txConfigBuilder.withTimeout(Duration.ofSeconds(request.maxExecutionTime()));
        }
        return txConfigBuilder.build();
    }

    private static AuthToken extractAuthToken(HttpServletRequest request) {
        // Auth has already passed through AuthorizationEnabledFilter, so we know we have formatted credential
        var authHeader = request.getHeader("Authorization");

        if (authHeader == null) {
            return AuthTokens.none();
        }

        var decoded = AuthorizationHeaders.decode(authHeader);
        if (decoded == null) {
            return AuthTokens.none();
        }

        return switch (decoded.scheme()) {
            case BEARER -> AuthTokens.bearer(decoded.values()[0]);
            case BASIC -> AuthTokens.basic(decoded.values()[0], decoded.values()[1]);
            default -> AuthTokens.none();
        };
    }

    private static String randomTxId(Integer length) {
        // Available chars 0-9 and a-z.
        return Long.toHexString(ThreadLocalRandom.current().nextLong()).substring(0, length);
    }

    private static Response successWithResultResponse(
            Transaction transaction, boolean requireCounters, boolean requiresCommit) {
        return Response.accepted()
                .entity(new TxManagedResultContainer(transaction, requireCounters, requiresCommit))
                .build();
    }

    private static Response txConcurrentAccessResponse() {
        return Response.status(Response.Status.BAD_REQUEST)
                .entity(singleError(
                        TransactionAccessedConcurrently.code().serialize(),
                        "Another request is currently accessing this transaction."))
                .build();
    }

    private static Response bookmarksOnlyResponse(Set<Bookmark> bookmarks) {
        return Response.accepted().entity(fromBookmarks(bookmarks)).build();
    }

    private static Response transactionInfoOnlyResponse(Transaction transaction) {
        return Response.accepted().entity(fromQueryAPITransaction(transaction)).build();
    }

    private static Response transactionNotFoundResponse(String transactionId) {
        return Response.status(404)
                .entity(singleError(
                        Status.Request.Invalid.code().serialize(),
                        format(
                                "Transaction with Id: \"%s\" was not found. It may have timed out and therefore"
                                        + " rolled back or the routing header \'neo4j-cluster-affinity\' was not provided.",
                                transactionId)))
                .build();
    }

    private static Response serverError() {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(singleError(
                        Status.General.UnknownError.code().serialize(),
                        Status.General.UnknownError.code().description()))
                .build();
    }

    private Response databaseError(DatabaseException databaseException) {
        return Response.serverError()
                .entity(fromDriverException(databaseException))
                .build();
    }

    private static Response clientError(Neo4jException clientException) {
        return Response.status(Response.Status.BAD_REQUEST)
                .entity(fromDriverException(clientException))
                .build();
    }

    private static Response notFoundDiscoveryResponse(FatalDiscoveryException ex) {
        return Response.status(Response.Status.NOT_FOUND)
                .entity(fromDriverException(ex))
                .build();
    }

    private static Response txCollisionResponse() {
        return Response.status(Response.Status.BAD_REQUEST)
                .entity(singleError(
                        Status.Request.ResourceExhaustion.code().serialize(),
                        "A transaction identifier collision has been detected whilst creating your"
                                + " transaction. Please retry. If this occurs frequently consider increasing"
                                + "the length of transaction identifier."))
                .build();
    }

    private Instant generateTimeout() {
        return Instant.now().truncatedTo(ChronoUnit.SECONDS).plus(defaultTimeout);
    }

    private enum TxHandling {
        RETURN,
        KEEP_OPEN,
        CLOSE
    }
}
