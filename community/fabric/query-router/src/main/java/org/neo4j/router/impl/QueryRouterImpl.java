/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.router.impl;

import java.util.function.Function;
import org.neo4j.configuration.Config;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.bookmark.TransactionBookmarkManagerImpl;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.ErrorReporter;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.router.QueryRouter;
import org.neo4j.router.impl.query.CompositeQueryTargetService;
import org.neo4j.router.impl.query.StandardQueryTargetService;
import org.neo4j.router.impl.transaction.RouterTransactionContextImpl;
import org.neo4j.router.impl.transaction.RouterTransactionImpl;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.DatabaseReferenceResolver;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryTargetParser;
import org.neo4j.router.query.QueryTargetService;
import org.neo4j.router.transaction.DatabaseTransactionFactory;
import org.neo4j.router.transaction.RouterTransactionContext;
import org.neo4j.router.transaction.RoutingInfo;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.time.SystemNanoClock;

public class QueryRouterImpl implements QueryRouter {

    private final QueryTargetParser queryTargetParser;
    private final DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory;
    private final DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory;
    private final Function<RoutingInfo, LocationService> locationServiceFactory;
    private final Config config;
    private final DatabaseReferenceResolver databaseReferenceResolver;
    private final ErrorReporter errorReporter;
    private final SystemNanoClock systemNanoClock;

    public QueryRouterImpl(
            Config config,
            DatabaseReferenceResolver databaseReferenceResolver,
            Function<RoutingInfo, LocationService> locationServiceFactory,
            QueryTargetParser queryTargetParser,
            DatabaseTransactionFactory<Location.Local> localDatabaseTransactionFactory,
            DatabaseTransactionFactory<Location.Remote> remoteDatabaseTransactionFactory,
            ErrorReporter errorReporter,
            SystemNanoClock systemNanoClock) {
        this.config = config;
        this.databaseReferenceResolver = databaseReferenceResolver;
        this.locationServiceFactory = locationServiceFactory;
        this.queryTargetParser = queryTargetParser;
        this.localDatabaseTransactionFactory = localDatabaseTransactionFactory;
        this.remoteDatabaseTransactionFactory = remoteDatabaseTransactionFactory;
        this.errorReporter = errorReporter;
        this.systemNanoClock = systemNanoClock;
    }

    @Override
    public RouterTransactionContext beginTransaction(TransactionInfo incomingTransactionInfo) {
        var transactionBookmarkManager =
                new TransactionBookmarkManagerImpl(BookmarkFormat.parse(incomingTransactionInfo.bookmarks()));
        var transactionInfo = incomingTransactionInfo.withDefaults(config);
        var sessionDatabaseReference = resolveSessionDatabaseReference(transactionInfo);
        var routingInfo = new RoutingInfo(
                sessionDatabaseReference, transactionInfo.routingContext(), transactionInfo.accessMode());
        var queryTargetService = createQueryTargetService(routingInfo);
        var locationService = createLocationService(routingInfo);
        var routerTransaction = createRouterTransaction(transactionInfo, transactionBookmarkManager);
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

    private QueryTargetService createQueryTargetService(RoutingInfo routingInfo) {
        var sessionDatabaseReference = routingInfo.sessionDatabaseReference();
        if (sessionDatabaseReference.isComposite()) {
            return new CompositeQueryTargetService(sessionDatabaseReference);
        } else {
            return new StandardQueryTargetService(
                    sessionDatabaseReference, queryTargetParser, databaseReferenceResolver);
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
                transactionBookmarkManager);
    }

    @Override
    public QueryExecution executeQuery(RouterTransactionContext context, Query query, QuerySubscriber subscriber) {
        var target = context.queryTargetService().determineTarget(query);
        var location = context.locationService().locationOf(target);
        var databaseTransaction = context.transactionFor(location);
        return databaseTransaction.executeQuery(query, subscriber);
    }
}
