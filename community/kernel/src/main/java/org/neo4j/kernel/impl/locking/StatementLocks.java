package org.neo4j.kernel.impl.locking;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.KernelStatement;

/**
 * Component used by {@link KernelStatement} to acquire {@link #implicit() implicit} and {@link #explicit() explicit}
 * locks.
 */
public interface StatementLocks extends AutoCloseable
{
    /**
     * Get {@link Locks.Client} responsible for explicit locks. Such locks are explicitly grabbed by the user via
     * {@link Transaction#acquireReadLock(PropertyContainer)} and
     * {@link Transaction#acquireWriteLock(PropertyContainer)}.
     *
     * @return the locks client to serve explicit locks.
     */
    Locks.Client explicit();

    /**
     * Get {@link Locks.Client} responsible for implicit locks. Such locks are grabbed by the database itself to
     * provide consistency guarantees.
     *
     * @return the locks client to serve implicit locks.
     */
    Locks.Client implicit();

    /**
     * Prepare the underlying {@link Locks.Client client}(s) for commit.
     */
    void prepareForCommit();

    /**
     * Stop the underlying {@link Locks.Client client}(s).
     */
    void stop();

    /**
     * Close the underlying {@link Locks.Client client}(s).
     */
    @Override
    void close();
}
