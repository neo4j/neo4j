package org.neo4j.onlinebackup.net;

public class SocketException extends RuntimeException
{
    public SocketException( String message ) 
    {
        super( message );
    }
    
    public SocketException( String message, Throwable cause ) 
    {
        super( message, cause );
    }
}
