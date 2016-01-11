package org.neo4j.kernel.configuration;

public class IllegalResourceException extends RuntimeException
{
    public IllegalResourceException( String message )
    {
        super( message );
    }
}
