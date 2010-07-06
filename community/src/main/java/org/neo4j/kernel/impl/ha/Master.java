package org.neo4j.kernel.impl.ha;

import org.neo4j.graphdb.PropertyContainer;

/**
 * Represents the master-side of the HA communication between master and slave.
 * A master will receive calls to these methods from slaves when they do stuff.
 */
public interface Master
{
    Response<IdAllocation> allocateIds( SlaveContext context, Class<?> cls );

    Response<Integer> createRelationshipType( SlaveContext context, String name );

    Response<LockResult> acquireWriteLock( SlaveContext context, int localTxId,
            PropertyContainer... entities );

    Response<LockResult> acquireReadLock( SlaveContext context, int localTxId,
            PropertyContainer... entities );

    Response<Integer> commitTransaction( SlaveContext context,
            int localTxId, TransactionStream transaction );

    Response<Void> rollbackTransaction( SlaveContext context, int localTxId );

    Response<Void> pullUpdates( SlaveContext context );
}
