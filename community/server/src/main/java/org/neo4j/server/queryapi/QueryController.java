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

import static org.neo4j.server.queryapi.request.AccessMode.toDriverAccessMode;
import static org.neo4j.server.queryapi.response.QueryResponseBookmarks.fromBookmarks;
import static org.neo4j.server.queryapi.response.QueryResponseTxInfo.fromQueryAPITransaction;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.server.queryapi.exception.QueryApiException;
import org.neo4j.server.queryapi.exception.TransactionConcurrentAccessException;
import org.neo4j.server.queryapi.exception.TransactionNotFoundException;
import org.neo4j.server.queryapi.request.QueryRequest;
import org.neo4j.server.queryapi.response.QueryResponseAutoCommit;
import org.neo4j.server.queryapi.response.QueryResponseTimers;
import org.neo4j.server.queryapi.response.QueryResponseTxManaged;
import org.neo4j.server.queryapi.tx.Transaction;
import org.neo4j.server.queryapi.tx.TransactionManager;
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

        var txConfig = buildTxConfig(request);
        try {
            var timers = QueryResponseTimers.start();
            var result = session.run(request.statement(), request.parametersOrSupplied(Map::of), txConfig);
            timers.notifyResultAvailable();
            var resultContainer = new QueryResponseAutoCommit(result, session, timers, request.includeCounters());
            return Response.accepted(resultContainer).build();
        } catch (Neo4jException neo4jException) {
            throw neo4jException;
        } catch (Exception exception) {
            log.error("Local driver failed to execute query", exception);
            throw exception;
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
        Transaction transaction;
        try {
            transaction = transactionManager.begin(
                    txId, session, sessionAuthToken, databaseName, buildTxConfig(request), request.txType());
        } catch (QueryApiException | Neo4jException queryApiException) {
            throw queryApiException;
        } catch (Exception exception) {
            log.error("Local driver failed to execute query", exception);
            throw exception;
        }

        return executeStatement(request, transaction, false);
    }

    public Response continueTransaction(
            QueryRequest request, HttpServletRequest rawRequest, String databaseName, String txId) {
        return executeStatement(request, txId, extractAuthToken(rawRequest), databaseName, false);
    }

    public Response commitTransaction(
            QueryRequest request, HttpServletRequest rawRequest, String databaseName, String txId) {
        return executeStatement(request, txId, extractAuthToken(rawRequest), databaseName, true);
    }

    public Response rollbackTransaction(String txId, HttpServletRequest rawRequest, String requestDatabase)
            throws QueryApiException {
        Transaction transaction;
        try {
            transaction = transactionManager.retrieveTransaction(txId, requestDatabase, extractAuthToken(rawRequest));
        } catch (WrongUserException ex) {
            throw new TransactionNotFoundException(txId);
        } catch (ConcurrentModificationException ex) {
            throw new TransactionConcurrentAccessException();
        }

        try {
            transaction.rollback(); // todo handle rollback failure
        } finally {
            transactionManager.removeTransaction(txId);
        }
        // Keeps the status as 200 for not breaking compatibility
        return Response.ok().entity(fromBookmarks(null)).build();
    }

    private Response executeStatement(
            QueryRequest request,
            String txId,
            AuthToken requestAuthToken,
            String requestDatabase,
            boolean requiresCommit)
            throws QueryApiException {
        Transaction queryAPITransaction;

        try {
            queryAPITransaction = transactionManager.retrieveTransaction(txId, requestDatabase, requestAuthToken);
        } catch (WrongUserException ex) {
            throw new TransactionNotFoundException(txId);
        }

        return executeStatement(request, queryAPITransaction, requiresCommit);
    }

    private Response executeStatement(QueryRequest request, Transaction transaction, boolean requiresCommit)
            throws QueryApiException {
        var txCleanUpAction = TxHandling.CLOSE;

        try {
            if (request.statement() != null && !request.statement().isEmpty()) {
                var timers = QueryResponseTimers.start();
                var result = transaction.run(request.statement(), request.parameters());
                timers.notifyResultAvailable();
                txCleanUpAction = TxHandling.KEEP_OPEN;
                return successWithResultResponse(
                        result, transaction, timers, request.includeCounters(), requiresCommit);
            } else {
                if (requiresCommit) {
                    var bookmarks = transaction.commit();
                    return bookmarksOnlyResponse(bookmarks);
                } else {
                    transaction.extendTimeout();
                    txCleanUpAction = TxHandling.RETURN;
                    return transactionInfoOnlyResponse(transaction);
                }
            }
        } catch (Neo4jException neo4jException) {
            throw neo4jException;
        } catch (Exception exception) {
            log.error("Local driver failed to execute query", exception);
            throw exception;
        } finally {
            cleanUp(transaction.id(), txCleanUpAction);
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
        if (request.txMetadata() != null) {
            txConfigBuilder.withMetadata(request.txMetadata());
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
            Result result,
            Transaction transaction,
            QueryResponseTimers timers,
            boolean requireCounters,
            boolean requiresCommit) {
        return Response.accepted()
                .entity(new QueryResponseTxManaged(result, transaction, timers, requireCounters, requiresCommit))
                .build();
    }

    private static Response bookmarksOnlyResponse(Set<Bookmark> bookmarks) {
        return Response.accepted().entity(fromBookmarks(bookmarks)).build();
    }

    private static Response transactionInfoOnlyResponse(Transaction transaction) {
        return Response.accepted().entity(fromQueryAPITransaction(transaction)).build();
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
