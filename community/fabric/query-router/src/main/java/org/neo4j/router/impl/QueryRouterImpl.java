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
import org.neo4j.cypher.internal.options.CypherExecutionMode;
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
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryRoutingMonitor;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.router.QueryRouter;
import org.neo4j.router.QueryRouterException;
import org.neo4j.router.impl.query.CompositeQueryPreParsedInfoService;
import org.neo4j.router.impl.query.StandardQueryPreParsedInfoService;
import org.neo4j.router.impl.query.StatementType;
import org.neo4j.router.impl.transaction.RouterTransactionContextImpl;
import org.neo4j.router.impl.transaction.RouterTransactionImpl;
import org.neo4j.router.impl.transaction.RouterTransactionManager;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.DatabaseReferenceResolver;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryPreParsedInfoParser;
import org.neo4j.router.query.QueryPreParsedInfoService;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.RouterTransactionContext;
import org.neo4j.router.transaction.RoutingInfo;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.time.SystemNanoClock;

public class QueryRouterImpl implements QueryRouter {

    private final QueryPreParsedInfoParser queryPreParsedInfoParser;
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

    public QueryRouterImpl(
            Config config,
            DatabaseReferenceResolver databaseReferenceResolver,
            Function<RoutingInfo, LocationService> locationServiceFactory,
            QueryPreParsedInfoParser queryTargetParser,
            DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory,
            DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory,
            ErrorReporter errorReporter,
            SystemNanoClock systemNanoClock,
            LocalGraphTransactionIdTracker transactionIdTracker,
            QueryStatementLifecycles statementLifecycles,
            QueryRoutingMonitor queryRoutingMonitor,
            RouterTransactionManager transactionManager,
            AbstractSecurityLog securityLog) {
        this.config = config;
        this.databaseReferenceResolver = databaseReferenceResolver;
        this.locationServiceFactory = locationServiceFactory;
        this.queryPreParsedInfoParser = queryTargetParser;
        this.localDatabaseTransactionFactory = localDatabaseTransactionFactory;
        this.remoteDatabaseTransactionFactory = remoteDatabaseTransactionFactory;
        this.errorReporter = errorReporter;
        this.systemNanoClock = systemNanoClock;
        this.transactionIdTracker = transactionIdTracker;
        this.statementLifecycles = statementLifecycles;
        this.queryRoutingMonitor = queryRoutingMonitor;
        this.transactionManager = transactionManager;
        this.securityLog = securityLog;
    }

    @Override
    public RouterTransactionContext beginTransaction(TransactionInfo incomingTransactionInfo) {
        var transactionBookmarkManager =
                new TransactionBookmarkManagerImpl(BookmarkFormat.parse(incomingTransactionInfo.bookmarks()));
        // regardless of what we do, System graph must be always up to date
        transactionBookmarkManager
                .getBookmarkForLocalSystemDatabase()
                .ifPresent(
                        localBookmark -> transactionIdTracker.awaitSystemGraphUpToDate(localBookmark.transactionId()));
        var transactionInfo = incomingTransactionInfo.withDefaults(config);
        var sessionDatabaseReference = resolveSessionDatabaseReference(transactionInfo);
        authorize(sessionDatabaseReference, incomingTransactionInfo.loginContext());
        var routingInfo = new RoutingInfo(
                sessionDatabaseReference, transactionInfo.routingContext(), transactionInfo.accessMode());
        var queryTargetService = createQueryPreParsedInfoService(routingInfo);
        var locationService = createLocationService(routingInfo);
        var routerTransaction = createRouterTransaction(transactionInfo, transactionBookmarkManager);
        transactionManager.registerTransaction(routerTransaction);
        return new RouterTransactionContextImpl(
                transactionInfo,
                routingInfo,
                routerTransaction,
                queryTargetService,
                locationService,
                transactionBookmarkManager);
    }

    private DatabaseReference resolveSessionDatabaseReference(TransactionInfo transactionInfo) {
        var sessionDatabaseName = transactionInfo.sessionDatabaseName();
        return databaseReferenceResolver.resolve(sessionDatabaseName);
    }

    private QueryPreParsedInfoService createQueryPreParsedInfoService(RoutingInfo routingInfo) {
        var sessionDatabaseReference = routingInfo.sessionDatabaseReference();
        if (sessionDatabaseReference.isComposite()) {
            return new CompositeQueryPreParsedInfoService(sessionDatabaseReference);
        } else {
            return new StandardQueryPreParsedInfoService(sessionDatabaseReference, databaseReferenceResolver);
        }
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
        var statementLifecycle = statementLifecycles.create(
                context.transactionInfo().statementLifecycleTransactionInfo(), query.text(), query.parameters(), null);
        statementLifecycle.startProcessing();
        try {
            var preParsedInfo = queryPreParsedInfoParser.parseQuery(query);
            StatementType statementType = preParsedInfo.statementType();
            CypherExecutionMode executionMode = preParsedInfo.cypherExecutionMode();
            AccessMode accessMode = context.transactionInfo().accessMode();
            context.verifyStatementType(statementType);
            var target = context.preParsedInfo().target(preParsedInfo);
            var location = context.locationService().locationOf(target);
            verifyAccessModeWithStatementType(executionMode, accessMode, statementType, location);
            updateQueryRouterMetric(location);
            var databaseTransaction =
                    context.transactionFor(location, transactionMode(accessMode, statementType, executionMode));
            statementLifecycle.doneRouterProcessing(
                    preParsedInfo.obfuscationMetadata().get(), target.isComposite());
            return databaseTransaction.executeQuery(query, subscriber, statementLifecycle);
        } catch (RuntimeException e) {
            statementLifecycle.endFailure(e);

            throw e;
        }
    }

    private TransactionMode transactionMode(
            AccessMode accessMode, StatementType statementType, CypherExecutionMode executionMode) {
        if (accessMode == AccessMode.READ) {
            return TransactionMode.DEFINITELY_READ;
        } else if (statementType.isReadQuery() || executionMode.isExplain()) {
            return TransactionMode.MAYBE_WRITE;
        } else {
            return TransactionMode.DEFINITELY_WRITE;
        }
    }

    private void verifyAccessModeWithStatementType(
            CypherExecutionMode executionMode, AccessMode accessMode, StatementType statementType, Location location) {
        if (!(executionMode.isExplain())
                && accessMode == AccessMode.READ
                && statementType == StatementType.WRITE_QUERY) {
            throw new QueryRouterException(
                    Status.Statement.AccessMode,
                    WRITING_IN_READ_NOT_ALLOWED_MSG + ". Attempted write to %s",
                    location.getDatabaseName());
        }
    }

    @Override
    public long clearQueryCachesForDatabase(String databaseName) {
        return queryPreParsedInfoParser.clearQueryCachesForDatabase(databaseName);
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
