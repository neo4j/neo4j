package org.neo4j.kernel.impl.management;

import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.management.LockManager;

@Description( "Information about the Neo4j lock status" )
class LockManagerBean extends Neo4jMBean implements LockManager
{
    private final org.neo4j.kernel.impl.transaction.LockManager lockManager;

    LockManagerBean( int instanceId, org.neo4j.kernel.impl.transaction.LockManager lockManager )
            throws NotCompliantMBeanException
    {
        super( instanceId, LockManager.class );
        this.lockManager = lockManager;
    }

    @Description( "The number of lock sequences that would have lead to a deadlock situation that "
                  + "Neo4j has detected and adverted (by throwing DeadlockDetectedException)." )
    public long getNumberOfAdvertedDeadlocks()
    {
        return lockManager.getDetectedDeadlockCount();
    }
}
