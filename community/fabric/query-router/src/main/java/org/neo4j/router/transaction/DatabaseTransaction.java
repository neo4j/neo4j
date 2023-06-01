package org.neo4j.router.transaction;

import org.neo4j.fabric.transaction.parent.ChildTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.router.query.Query;

/**
 * Represents a transaction against a single database
 */
public interface DatabaseTransaction extends ChildTransaction {

    void commit();

    void rollback();

    void close();

    void terminate(Status reason);

    QueryExecution executeQuery(Query query, QuerySubscriber querySubscriber);
}
