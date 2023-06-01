package org.neo4j.router.transaction;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.QueryTargetService;

/**
 * Context object that contains transaction-specific information and services
 */
public interface RouterTransactionContext {

    TransactionInfo transactionInfo();

    QueryTargetService queryTargetService();

    LocationService locationService();

    /**
     * Begins a new, or reuses an existing, database transaction for the given location
     */
    DatabaseTransaction transactionFor(Location location);

    TransactionBookmarkManager txBookmarkManager();

    RouterTransaction routerTransaction();
}
