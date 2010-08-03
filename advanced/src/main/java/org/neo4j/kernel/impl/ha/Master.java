package org.neo4j.kernel.impl.ha;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.IdType;

/**
 * Represents the master-side of the HA communication between master and slave.
 * A master will receive calls to these methods from slaves when they do stuff.
 */
public interface Master
{
    Response<IdAllocation> allocateIds( SlaveContext context, IdType idType );

    Response<Integer> createRelationshipType( SlaveContext context, String name );

    Response<LockResult> acquireWriteLock( SlaveContext context, int identifier,
            Node... nodes );

    Response<LockResult> acquireReadLock( SlaveContext context, int identifier,
            Node... nodes );

    Response<LockResult> acquireWriteLock( SlaveContext context, int identifier,
            Relationship... relationships );

    Response<LockResult> acquireReadLock( SlaveContext context, int identifier,
            Relationship... relationships );
    
//    Response<Collection<Pair<String, Integer>>> commitTransaction( SlaveContext context,
//            int localTxId, Collection<Pair<String, TransactionStream>> transactionStreams );
    
    Response<Long> commitSingleResourceTransaction( SlaveContext context,
            int identifier, String resource, TransactionStream transactionStream );

    Response<Void> rollbackTransaction( SlaveContext context, int identifier );

    Response<Void> pullUpdates( SlaveContext context );
}
