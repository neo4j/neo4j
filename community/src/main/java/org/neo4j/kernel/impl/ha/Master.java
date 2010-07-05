package org.neo4j.kernel.impl.ha;

import org.neo4j.graphdb.PropertyContainer;

/**
 * Represents the master-side of the HA communication between master and slave.
 * A master will receive calls to these methods from slaves when they do stuff.
 */
public interface Master
{
    Response<IdRange> allocateNodeIds( SlaveContext context );

    Response<IdRange> allocateRelationshipIds( SlaveContext context );

    Response<IdRange> allocatePropertyIds( SlaveContext context );

    Response<Integer> createRelationshipType( SlaveContext context, String name );

    Response<LockStatus> acquireWriteLock( SlaveContext context, int localTxId,
            PropertyContainer... entities );

    Response<LockStatus> acquireReadLock( SlaveContext context, int localTxId,
            PropertyContainer... entities );

    Response<TransactionResult> commitTransaction( SlaveContext context,
            int localTxId, TransactionStream transaction );

    Response<TransactionStatus> rollbackTransaction( SlaveContext context,
            int localTxId );

    TransactionStream pullUpdates( SlaveContext context );
}
