package org.neo4j.kernel.ha.zookeeper;

public class ZooKeeperTimedOutException extends ZooKeeperException
{
    public ZooKeeperTimedOutException( String message )
    {
        super( message );
    }
}
