package org.neo4j.router.impl.transaction;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.QueryTargetService;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.RouterTransaction;
import org.neo4j.router.transaction.RouterTransactionContext;
import org.neo4j.router.transaction.RoutingInfo;
import org.neo4j.router.transaction.TransactionInfo;

public record RouterTransactionContextImpl(
        TransactionInfo transactionInfo,
        RoutingInfo routingInfo,
        RouterTransaction routerTransaction,
        QueryTargetService queryTargetService,
        LocationService locationService,
        TransactionBookmarkManager txBookmarkManager)
        implements RouterTransactionContext {

    @Override
    public TransactionInfo transactionInfo() {
        return transactionInfo;
    }

    @Override
    public QueryTargetService queryTargetService() {
        return queryTargetService;
    }

    @Override
    public LocationService locationService() {
        return locationService;
    }

    @Override
    public DatabaseTransaction transactionFor(Location location) {
        return routerTransaction.transactionFor(location);
    }

    @Override
    public TransactionBookmarkManager txBookmarkManager() {
        return txBookmarkManager;
    }

    @Override
    public RouterTransaction routerTransaction() {
        return routerTransaction;
    }
}
