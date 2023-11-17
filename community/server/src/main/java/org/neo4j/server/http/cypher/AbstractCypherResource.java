/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.cypher;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNullElse;
import static org.neo4j.server.rest.dbms.AuthorizedRequestWrapper.getLoginContextFromHttpServletRequest;
import static org.neo4j.server.web.HttpHeaderUtils.getAccessMode;
import static org.neo4j.server.web.HttpHeaderUtils.getBookmarks;
import static org.neo4j.server.web.HttpHeaderUtils.getTransactionTimeout;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import jakarta.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.http.cypher.format.api.InputEventStream;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;
import org.neo4j.server.rest.Neo4jError;
import org.neo4j.time.SystemNanoClock;

public abstract class AbstractCypherResource {
    private final HttpTransactionManager httpTransactionManager;
    private final TransactionUriScheme uriScheme;
    private final MemoryPool memoryPool;
    private final InternalLog log;
    private final String databaseName;
    private final SystemNanoClock clock;

    AbstractCypherResource(
            HttpTransactionManager httpTransactionManager,
            UriInfo uriInfo,
            MemoryPool memoryPool,
            InternalLog log,
            String databaseName,
            SystemNanoClock clock) {
        this.httpTransactionManager = httpTransactionManager;
        this.databaseName = databaseName;
        this.uriScheme = new TransactionUriBuilder(dbUri(uriInfo, databaseName), cypherUri(uriInfo, databaseName));
        this.memoryPool = memoryPool;
        this.log = log;
        this.clock = clock;
    }

    protected abstract URI dbUri(UriInfo uriInfo, String databaseName);

    protected abstract URI cypherUri(UriInfo uriInfo, String databaseName);

    @POST
    public Response executeStatementsInNewTransaction(
            InputEventStream inputEventStream, @Context HttpServletRequest request, @Context HttpHeaders headers) {
        try (var memoryTracker = createMemoryTracker()) {
            InputEventStream inputStream = ensureNotNull(inputEventStream);

            try {
                var graphDatabaseAPI = httpTransactionManager.getGraphDatabaseAPI(databaseName);
                return graphDatabaseAPI
                        .map(databaseAPI -> {
                            if (isDatabaseNotAvailable(databaseAPI)) {
                                return createNonAvailableDatabaseResponse(inputStream.getParameters());
                            }

                            memoryTracker.allocateHeap(Invocation.SHALLOW_SIZE);

                            final TransactionFacade transactionFacade = httpTransactionManager.createTransactionFacade(
                                    databaseAPI, memoryTracker, databaseName);
                            TransactionHandle transactionHandle = createNewTransactionHandle(
                                    transactionFacade, request, headers, memoryTracker, false);

                            Invocation invocation = new Invocation(
                                    log,
                                    transactionHandle,
                                    uriScheme.txCommitUri(transactionHandle.getId()),
                                    memoryPool,
                                    inputStream,
                                    false);
                            OutputEventStreamImpl outputStream = new OutputEventStreamImpl(
                                    inputStream.getParameters(), uriScheme, invocation::execute);
                            return Response.created(transactionHandle.uri())
                                    .entity(outputStream)
                                    .build();
                        })
                        .orElse(createNonExistentDatabaseResponse(inputStream.getParameters()));
            } catch (IllegalArgumentException ex) {
                return createInvalidAccessModeHeaderResponse(ex);
            } catch (RuntimeException ex) {
                if (ex instanceof Status.HasStatus) {
                    return createGenericErrorDatabaseResponse(
                            inputStream.getParameters(), ((Status.HasStatus) ex).status(), ex.getMessage());
                }

                throw ex;
            }
        }
    }

    @POST
    @Path("/{id}")
    public Response executeStatements(
            @PathParam("id") long id, InputEventStream inputEventStream, @Context HttpServletRequest request) {
        try (var memoryTracker = createMemoryTracker()) {
            return executeInExistingTransaction(
                    id, inputEventStream, memoryTracker, false, getLoginContextFromHttpServletRequest(request));
        }
    }

    @POST
    @Path("/{id}/commit")
    public Response commitTransaction(
            @PathParam("id") long id, InputEventStream inputEventStream, @Context HttpServletRequest request) {
        try (var memoryTracker = createMemoryTracker()) {
            return executeInExistingTransaction(
                    id, inputEventStream, memoryTracker, true, getLoginContextFromHttpServletRequest(request));
        }
    }

    @POST
    @Path("/commit")
    public Response commitNewTransaction(
            InputEventStream inputEventStream, @Context HttpServletRequest request, @Context HttpHeaders headers) {
        try (var memoryTracker = createMemoryTracker()) {
            InputEventStream inputStream = ensureNotNull(inputEventStream);

            try {
                Optional<GraphDatabaseAPI> graphDatabaseAPI = httpTransactionManager.getGraphDatabaseAPI(databaseName);
                return graphDatabaseAPI
                        .map(databaseAPI -> {
                            if (isDatabaseNotAvailable(databaseAPI)) {
                                return createNonAvailableDatabaseResponse(inputStream.getParameters());
                            }

                            memoryTracker.allocateHeap(Invocation.SHALLOW_SIZE);

                            final TransactionFacade transactionFacade = httpTransactionManager.createTransactionFacade(
                                    databaseAPI, memoryTracker, databaseName);
                            TransactionHandle transactionHandle = createNewTransactionHandle(
                                    transactionFacade, request, headers, memoryTracker, true);

                            Invocation invocation =
                                    new Invocation(log, transactionHandle, null, memoryPool, inputStream, true);
                            OutputEventStreamImpl outputStream = new OutputEventStreamImpl(
                                    inputStream.getParameters(), uriScheme, invocation::execute);
                            return Response.ok(outputStream).build();
                        })
                        .orElse(createNonExistentDatabaseResponse(inputStream.getParameters()));
            } catch (IllegalArgumentException ex) {
                return createInvalidAccessModeHeaderResponse(ex);
            } catch (RuntimeException ex) {
                if (ex instanceof Status.HasStatus) {
                    return createGenericErrorDatabaseResponse(
                            inputStream.getParameters(), ((Status.HasStatus) ex).status(), ex.getMessage());
                }

                throw ex;
            }
        }
    }

    @DELETE
    @Path("/{id}")
    public Response rollbackTransaction(@PathParam("id") final long id, @Context HttpServletRequest request) {
        try (var memoryTracker = createMemoryTracker()) {
            Optional<GraphDatabaseAPI> graphDatabaseAPI = httpTransactionManager.getGraphDatabaseAPI(databaseName);
            return graphDatabaseAPI
                    .map(databaseAPI -> {
                        if (isDatabaseNotAvailable(databaseAPI)) {
                            return createNonAvailableDatabaseResponse(emptyMap());
                        }

                        memoryTracker.allocateHeap(RollbackInvocation.SHALLOW_SIZE);

                        final TransactionFacade transactionFacade = httpTransactionManager.createTransactionFacade(
                                databaseAPI, memoryTracker, databaseName);

                        TransactionHandle transactionHandle;
                        try {
                            transactionHandle =
                                    transactionFacade.terminate(id, getLoginContextFromHttpServletRequest(request));
                        } catch (TransactionLifecycleException e) {
                            return invalidTransaction(e, emptyMap());
                        }

                        RollbackInvocation invocation = new RollbackInvocation(log, transactionHandle);
                        OutputEventStreamImpl outputEventStream =
                                new OutputEventStreamImpl(emptyMap(), uriScheme, invocation::execute);
                        return Response.ok().entity(outputEventStream).build();
                    })
                    .orElse(createNonExistentDatabaseResponse(emptyMap()));
        } catch (RuntimeException ex) {
            if (ex instanceof Status.HasStatus) {
                return createGenericErrorDatabaseResponse(
                        emptyMap(), ((Status.HasStatus) ex).status(), ex.getMessage());
            }

            throw ex;
        }
    }

    private MemoryTracker createMemoryTracker() {
        return new LocalMemoryTracker(memoryPool, 0, 64, null);
    }

    private static boolean isDatabaseNotAvailable(GraphDatabaseAPI databaseAPI) {
        return !databaseAPI.isAvailable();
    }

    private TransactionHandle createNewTransactionHandle(
            TransactionFacade transactionFacade,
            HttpServletRequest request,
            HttpHeaders headers,
            MemoryTracker memoryTracker,
            boolean implicitTransaction) {
        LoginContext loginContext = getLoginContextFromHttpServletRequest(request);
        long customTransactionTimeout = getTransactionTimeout(headers, log);
        var isReadOnlyTransaction = getAccessMode(headers);
        var bookmarks = getBookmarks(headers);

        if (isReadOnlyTransaction.isPresent()) {
            return transactionFacade.newTransactionHandle(
                    uriScheme,
                    implicitTransaction,
                    loginContext,
                    loginContext.connectionInfo(),
                    memoryTracker,
                    customTransactionTimeout,
                    clock,
                    isReadOnlyTransaction.get(),
                    bookmarks);
        } else {
            return transactionFacade.newTransactionHandle(
                    uriScheme,
                    implicitTransaction,
                    loginContext,
                    loginContext.connectionInfo(),
                    memoryTracker,
                    customTransactionTimeout,
                    bookmarks);
        }
    }

    private Response executeInExistingTransaction(
            long transactionId,
            InputEventStream inputEventStream,
            MemoryTracker memoryTracker,
            boolean finishWithCommit,
            LoginContext requestingUserLoginContext) {
        InputEventStream inputStream = ensureNotNull(inputEventStream);

        try {
            Optional<GraphDatabaseAPI> graphDatabaseAPI = httpTransactionManager.getGraphDatabaseAPI(databaseName);
            return graphDatabaseAPI
                    .map(databaseAPI -> {
                        if (isDatabaseNotAvailable(databaseAPI)) {
                            return createNonAvailableDatabaseResponse(inputStream.getParameters());
                        }

                        memoryTracker.allocateHeap(Invocation.SHALLOW_SIZE);

                        final TransactionFacade transactionFacade = httpTransactionManager.createTransactionFacade(
                                databaseAPI, memoryTracker, databaseName);

                        TransactionHandle transactionHandle;
                        try {
                            transactionHandle =
                                    transactionFacade.findTransactionHandle(transactionId, requestingUserLoginContext);
                        } catch (TransactionLifecycleException e) {
                            return invalidTransaction(e, inputStream.getParameters());
                        }

                        Invocation invocation = new Invocation(
                                log,
                                transactionHandle,
                                uriScheme.txCommitUri(transactionHandle.getId()),
                                memoryPool,
                                inputStream,
                                finishWithCommit);
                        OutputEventStreamImpl outputEventStream =
                                new OutputEventStreamImpl(inputStream.getParameters(), uriScheme, invocation::execute);

                        return Response.ok(outputEventStream).build();
                    })
                    .orElse(createNonExistentDatabaseResponse(inputStream.getParameters()));
        } catch (RuntimeException ex) {
            if (ex instanceof Status.HasStatus) {
                return createGenericErrorDatabaseResponse(
                        inputStream.getParameters(), ((Status.HasStatus) ex).status(), ex.getMessage());
            }

            throw ex;
        }
    }

    private Response invalidTransaction(TransactionLifecycleException e, Map<String, Object> parameters) {
        ErrorInvocation errorInvocation = new ErrorInvocation(e.toNeo4jError());
        return Response.status(Response.Status.NOT_FOUND)
                .entity(new OutputEventStreamImpl(parameters, uriScheme, errorInvocation::execute))
                .build();
    }

    private static InputEventStream ensureNotNull(InputEventStream inputEventStream) {
        return requireNonNullElse(inputEventStream, InputEventStream.EMPTY);
    }

    private Response createGenericErrorDatabaseResponse(Map<String, Object> parameters, Status status, String msg) {
        ErrorInvocation errorInvocation = new ErrorInvocation(new Neo4jError(status, msg));
        return Response.ok(new OutputEventStreamImpl(parameters, uriScheme, errorInvocation::execute))
                .build();
    }

    private Response createNonExistentDatabaseResponse(Map<String, Object> parameters) {
        ErrorInvocation errorInvocation = new ErrorInvocation(new Neo4jError(
                Status.Database.DatabaseNotFound,
                String.format("The database requested does not exists. Requested database name: '%s'.", databaseName)));
        return Response.status(Response.Status.NOT_FOUND)
                .entity(new OutputEventStreamImpl(parameters, uriScheme, errorInvocation::execute))
                .build();
    }

    private Response createNonAvailableDatabaseResponse(Map<String, Object> parameters) {
        ErrorInvocation errorInvocation = new ErrorInvocation(new Neo4jError(
                Status.General.DatabaseUnavailable,
                String.format("Requested database is not available. Requested database name: '%s'.", databaseName)));
        return Response.status(Response.Status.NOT_FOUND)
                .entity(new OutputEventStreamImpl(parameters, uriScheme, errorInvocation::execute))
                .build();
    }

    private Response createInvalidAccessModeHeaderResponse(IllegalArgumentException ex) {
        var errorInvocation = new ErrorInvocation(new Neo4jError(Status.Request.InvalidFormat, ex.getMessage()));
        return Response.status(Response.Status.OK)
                .entity(new OutputEventStreamImpl(emptyMap(), uriScheme, errorInvocation::execute))
                .build();
    }

    private static class TransactionUriBuilder implements TransactionUriScheme {
        private final URI dbUri;
        private final URI cypherUri;

        TransactionUriBuilder(URI dbUri, URI cypherUri) {
            this.dbUri = dbUri;
            this.cypherUri = cypherUri;
        }

        @Override
        public URI txUri(long id) {
            return transactionBuilder(id).build();
        }

        @Override
        public URI txCommitUri(long id) {
            return transactionBuilder(id).path("/commit").build();
        }

        @Override
        public URI dbUri() {
            return dbUri;
        }

        private UriBuilder transactionBuilder(long id) {
            return UriBuilder.fromUri(cypherUri).path("/" + id);
        }
    }
}
