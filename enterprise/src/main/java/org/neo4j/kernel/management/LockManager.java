package org.neo4j.kernel.management;

class LockManager extends Neo4jJmx implements LockManagerMBean
{
    private final org.neo4j.kernel.impl.transaction.LockManager lockManager;

    LockManager( int instanceId,
            org.neo4j.kernel.impl.transaction.LockManager lockManager )
    {
        super( instanceId );
        this.lockManager = lockManager;
    }

    public long getNumberOfAdvertedDeadlocks()
    {
        return lockManager.getDetectedDeadlockCount();
    }
}
