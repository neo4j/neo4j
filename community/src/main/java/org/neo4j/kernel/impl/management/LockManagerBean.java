package org.neo4j.kernel.impl.management;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.management.LockManager;

@Service.Implementation( ManagementBeanProvider.class )
public final class LockManagerBean extends ManagementBeanProvider
{
    public LockManagerBean()
    {
        super( LockManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new LockManagerImpl( this, kernel );
    }

    @Description( "Information about the Neo4j lock status" )
    private static class LockManagerImpl extends Neo4jMBean implements LockManager
    {
        private final org.neo4j.kernel.impl.transaction.LockManager lockManager;

        LockManagerImpl( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.lockManager = kernel.getConfig().getLockManager();
        }

        @Description( "The number of lock sequences that would have lead to a deadlock situation that "
                      + "Neo4j has detected and adverted (by throwing DeadlockDetectedException)." )
        public long getNumberOfAdvertedDeadlocks()
        {
            return lockManager.getDetectedDeadlockCount();
        }
    }
}
