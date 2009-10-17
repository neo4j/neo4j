package org.neo4j.ha;

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
