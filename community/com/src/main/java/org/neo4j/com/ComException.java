package org.neo4j.com;

public class ComException extends RuntimeException
{
    public ComException()
    {
        super();
    }

    public ComException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public ComException( String message )
    {
        super( message );
    }

    public ComException( Throwable cause )
    {
        super( cause );
    }
}
