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
package org.neo4j.router.impl;

import static org.neo4j.fabric.executor.FabricExecutor.WRITING_IN_READ_NOT_ALLOWED_MSG;

import java.util.function.Function;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.QueryOptions;
import org.neo4j.cypher.internal.options.CypherExecutionMode;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerImpl;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProviderFactory;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.logging.InternalLog;
import org.neo4j.router.QueryRouter;
import org.neo4j.router.QueryRouterException;
import org.neo4j.router.impl.query.ConstituentTransactionFactoryImpl;
import org.neo4j.router.impl.query.DirectTargetService;
import org.neo4j.router.impl.query.StandardTargetService;
import org.neo4j.router.impl.query.StatementType;
import org.neo4j.router.impl.query.TransactionTargetService;
import org.neo4j.router.impl.transaction.RouterTransactionContextImpl;
import org.neo4j.router.impl.transaction.RouterTransactionImpl;
import org.neo4j.router.impl.transaction.RouterTransactionManager;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.DatabaseReferenceResolver;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryProcessor;
import org.neo4j.router.query.TargetService;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.RouterTransaction;
import org.neo4j.router.transaction.RouterTransactionContext;
import org.neo4j.router.transaction.RoutingInfo;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.router.util.Errors;
import org.neo4j.time.SystemNanoClock;

public class QueryRouterImpl implements QueryRouter {

    private final QueryProcessor queryProcessor;
    private final DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory;
    private final DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory;
    private final Function<RoutingInfo, LocationService> locationServiceFactory;
    private final Config config;
    private final DatabaseReferenceResolver databaseReferenceResolver;
    private final ErrorReporter errorReporter;
    private final SystemNanoClock systemNanoClock;
    private final LocalGraphTransactionIdTracker transactionIdTracker;
    private final QueryStatementLifecycles statementLifecycles;
    private final RouterTransactionManager transactionManager;
    private final QueryRoutingMonitor queryRoutingMonitor;
    private final AbstractSecurityLog securityLog;
    private final InternalLog queryRouterLog;

    public QueryRouterImpl(
            Config config,
            DatabaseReferenceResolver databaseReferenceResolver,
            Function<RoutingInfo, LocationService> locationServiceFactory,
            QueryProcessor queryProcessor,
            DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory,
            DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory,
            ErrorReporter errorReporter,
            SystemNanoClock systemNanoClock,
            LocalGraphTransactionIdTracker transactionIdTracker,
            QueryStatementLifecycles statementLifecycles,
            QueryRoutingMonitor queryRoutingMonitor,
            RouterTransactionManager transactionManager,
            AbstractSecurityLog securityLog,
            InternalLog queryRouterLog) {
        this.config = config;
        this.databaseReferenceResolver = databaseReferenceResolver;
        this.locationServiceFactory = locationServiceFactory;
        this.queryProcessor = queryProcessor;
        this.localDatabaseTransactionFactory = localDatabaseTransactionFactory;
        this.remoteDatabaseTransactionFactory = remoteDatabaseTransactionFactory;
        this.errorReporter = errorReporter;
        this.systemNanoClock = systemNanoClock;
        this.transactionIdTracker = transactionIdTracker;
        this.statementLifecycles = statementLifecycles;
        this.queryRoutingMonitor = queryRoutingMonitor;
        this.transactionManager = transactionManager;
        this.securityLog = securityLog;
        this.queryRouterLog = queryRouterLog;
    }

    @Override
    public RouterTransactionContext beginTransaction(TransactionInfo incomingTransactionInfo) {
        var transactionBookmarkManager =
                new TransactionBookmarkManagerImpl(BookmarkFormat.parse(incomingTransactionInfo.bookmarks()));
        // regardless of what we do, System graph must be always up-to-date
        transactionBookmarkManager
                .getBookmarkForLocalSystemDatabase()
                .ifPresent(
                        localBookmark -> transactionIdTracker.awaitSystemGraphUpToDate(localBookmark.transactionId()));
        var transactionInfo = incomingTransactionInfo.withDefaults(config);
        var sessionDatabaseReference = resolveSessionDatabaseReference(transactionInfo);
        authorize(sessionDatabaseReference, incomingTransactionInfo.loginContext());
        var routingInfo = new RoutingInfo(
                sessionDatabaseReference, transactionInfo.routingContext(), transactionInfo.accessMode());
        var queryTargetService = createTargetService(routingInfo);
        var locationService = createLocationService(routingInfo);
        var routerTransaction = createRouterTransaction(transactionInfo, transactionBookmarkManager);
        transactionManager.registerTransaction(routerTransaction);

        // Try to create a dummy kernel transaction so that this router transaction can be monitored
        try {
            var dummyTransactionMode = transactionInfo.accessMode().equals(AccessMode.READ)
                    ? TransactionMode.DEFINITELY_READ
                    : TransactionMode.MAYBE_WRITE;
            routerTransaction.transactionFor(
                    locationService.locationOf(sessionDatabaseReference), dummyTransactionMode, locationService);
        } catch (Exception e) {
            queryRouterLog.warn("Could not eagerly create kernel transaction due to: %s".formatted(e));
        }

        return new RouterTransactionContextImpl(
                transactionInfo,
                routingInfo,
                routerTransaction,
                new TransactionTargetService(queryTargetService),
                locationService,
                transactionBookmarkManager);
    }

    private DatabaseReference resolveSessionDatabaseReference(TransactionInfo transactionInfo) {
        var sessionDatabaseName = transactionInfo.sessionDatabaseName();
        return databaseReferenceResolver.resolve(sessionDatabaseName);
    }

    private TargetService createTargetService(RoutingInfo routingInfo) {
        var sessionDatabaseReference = routingInfo.sessionDatabaseReference();
        if (sessionDatabaseReference.isComposite()) {
            return new DirectTargetService(sessionDatabaseReference);
        } else {
            return new StandardTargetService(sessionDatabaseReference, databaseReferenceResolver);
        }
    }

    private CypherExecutionMode executionMode(QueryOptions queryOptions, Boolean isComposite) {
        CypherExecutionMode cypherExecutionMode = queryOptions.queryOptions().executionMode();
        if (isComposite && cypherExecutionMode.isProfile()) {
            Errors.semantic("'PROFILE' is not supported on composite databases.");
        }
        return cypherExecutionMode;
    }

    private LocationService createLocationService(RoutingInfo routingInfo) {
        return locationServiceFactory.apply(routingInfo);
    }

    private RouterTransactionImpl createRouterTransaction(
            TransactionInfo transactionInfo, TransactionBookmarkManager transactionBookmarkManager) {
        return new RouterTransactionImpl(
                transactionInfo,
                localDatabaseTransactionFactory,
                remoteDatabaseTransactionFactory,
                errorReporter,
                systemNanoClock,
                transactionBookmarkManager,
                TraceProviderFactory.getTraceProvider(config),
                transactionManager);
    }

    private void authorize(DatabaseReference sessionDb, LoginContext loginContext) {
        var databaseNameToAuthorizeFor = sessionDb instanceof DatabaseReferenceImpl.Internal
                ? ((DatabaseReferenceImpl.Internal) sessionDb).databaseId().name()
                : sessionDb.alias().name();

        loginContext.authorize(LoginContext.IdLookup.EMPTY, databaseNameToAuthorizeFor, securityLog);
    }

    @Override
    public QueryExecution executeQuery(RouterTransactionContext context, Query query, QuerySubscriber subscriber) {
        TransactionInfo transactionInfo = context.transactionInfo();
        var statementLifecycle = statementLifecycles.create(
                transactionInfo.statementLifecycleTransactionInfo(), query.text(), query.parameters(), null);
        statementLifecycle.startProcessing();
        try {
            LocationService locationService = context.locationService();
            var processedQueryInfo = queryProcessor.processQuery(
                    query,
                    context.targetService(),
                    locationService,
                    cancellationChecker(context.routerTransaction()),
                    context.transactionInfo().isComposite());
            StatementType statementType = processedQueryInfo.statementType();
            QueryOptions queryOptions = processedQueryInfo.queryOptions();
            CypherExecutionMode executionMode = executionMode(queryOptions, transactionInfo.isComposite());
            AccessMode accessMode = transactionInfo.accessMode();
            context.verifyStatementType(statementType);
            var target = processedQueryInfo.target();
            verifyAccessModeWithStatementType(executionMode, accessMode, statementType, target);
            var location = locationService.locationOf(target);
            updateQueryRouterMetric(location);
            statementLifecycle.doneRouterProcessing(
                    processedQueryInfo.obfuscationMetadata().get(), target.isComposite());

            RouterTransaction routerTransaction = context.routerTransaction();
            var constituentTransactionFactory = getConstituentTransactionFactory(context, queryOptions);
            routerTransaction.setConstituentTransactionFactory(constituentTransactionFactory);

            // uses routerTransaction to create transaction
            var databaseTransaction = context.transactionFor(
                    location,
                    TransactionMode.from(accessMode, executionMode, statementType.isReadQuery(), target.isComposite()));
            return databaseTransaction.executeQuery(
                    processedQueryInfo.rewrittenQuery(), subscriber, statementLifecycle);
        } catch (RuntimeException e) {
            statementLifecycle.endFailure(e);

            throw e;
        }
    }

    private CancellationChecker cancellationChecker(RouterTransaction routerTransaction) {
        return () ->
                routerTransaction.throwIfTerminatedOrClosed(() -> "Trying to process query in a closed transaction");
    }

    private ConstituentTransactionFactory getConstituentTransactionFactory(
            RouterTransactionContext context, QueryOptions queryOptions) {
        if (!(context.transactionInfo().isComposite())) {
            return ConstituentTransactionFactory.throwing();
        }
        return new ConstituentTransactionFactoryImpl(
                queryProcessor,
                statementLifecycles,
                cancellationChecker(context.routerTransaction()),
                queryOptions,
                context);
    }

    private void verifyAccessModeWithStatementType(
            CypherExecutionMode executionMode,
            AccessMode accessMode,
            StatementType statementType,
            DatabaseReference databaseReference) {
        if (!(executionMode.isExplain()) && accessMode == AccessMode.READ && statementType.isWrite()) {
            throw new QueryRouterException(
                    Status.Statement.AccessMode,
                    WRITING_IN_READ_NOT_ALLOWED_MSG + ". Attempted write to %s",
                    databaseReference.alias().name());
        }
    }

    @Override
    public long clearQueryCachesForDatabase(String databaseName) {
        return queryProcessor.clearQueryCachesForDatabase(databaseName);
    }

    private void updateQueryRouterMetric(Location location) {
        if (location instanceof Location.Local) {
            queryRoutingMonitor.queryRoutedLocal();
        } else if (location instanceof Location.Remote.Internal) {
            queryRoutingMonitor.queryRoutedRemoteInternal();
        } else {
            queryRoutingMonitor.queryRoutedRemoteExternal();
        }
    }
}
