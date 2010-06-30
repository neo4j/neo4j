package org.neo4j.kernel.impl.management;

import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.management.LockManager;

class LockManagerBean extends Neo4jMBean implements LockManager
{
    private final org.neo4j.kernel.impl.transaction.LockManager lockManager;

    LockManagerBean( int instanceId, org.neo4j.kernel.impl.transaction.LockManager lockManager )
            throws NotCompliantMBeanException
    {
        super( instanceId, LockManager.class );
        this.lockManager = lockManager;
    }

    public long getNumberOfAdvertedDeadlocks()
    {
        return lockManager.getDetectedDeadlockCount();
    }
}
