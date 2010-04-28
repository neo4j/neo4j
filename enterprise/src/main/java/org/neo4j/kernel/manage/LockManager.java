package org.neo4j.kernel.manage;

class LockManager extends Neo4jJmx implements LockManagerMBean
{
    LockManager( int instanceId )
    {
        super( instanceId );
    }

    public long getNumberOfAdvertedDeadlocks()
    {
        // TODO Auto-generated method stub
        return 0;
    }
}
