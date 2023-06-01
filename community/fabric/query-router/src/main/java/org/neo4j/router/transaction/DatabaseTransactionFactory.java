package org.neo4j.router.transaction;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.Location;

/**
 * A factory for starting database transactions at given locations
 */
public interface DatabaseTransactionFactory<LOC extends Location> {

    DatabaseTransaction beginTransaction(
            LOC location, TransactionInfo transactionInfo, TransactionBookmarkManager bookmarkManager);
}
