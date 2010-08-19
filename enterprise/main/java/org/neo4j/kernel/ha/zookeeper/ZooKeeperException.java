package org.neo4j.kernel.ha.zookeeper;

public class ZooKeeperException extends RuntimeException
{
    public ZooKeeperException( String message )
    {
        super( message );
    }
    
    public ZooKeeperException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
