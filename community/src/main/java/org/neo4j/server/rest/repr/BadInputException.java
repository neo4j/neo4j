package org.neo4j.server.rest.repr;

public class BadInputException extends Exception
{
    public BadInputException( Throwable cause )
    {
        super( cause.getMessage(), cause );
    }

    public BadInputException( String message )
    {
        super( message );
    }
}
