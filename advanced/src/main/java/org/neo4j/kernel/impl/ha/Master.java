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

    Response<LockResult> acquireWriteLock( SlaveContext context, int eventIdentifier,
            Node... nodes );

    Response<LockResult> acquireReadLock( SlaveContext context, int eventIdentifier,
            Node... nodes );

    Response<LockResult> acquireWriteLock( SlaveContext context, int eventIdentifier,
            Relationship... relationships );

    Response<LockResult> acquireReadLock( SlaveContext context, int eventIdentifier,
            Relationship... relationships );
    
    Response<Long> commitSingleResourceTransaction( SlaveContext context,
            int eventIdentifier, String resource, TransactionStream transactionStream );

    Response<Void> rollbackTransaction( SlaveContext context, int eventIdentifier );

    Response<Void> pullUpdates( SlaveContext context );
}
