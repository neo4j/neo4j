package org.neo4j.impl.nioneo.store;

public class InvalidIdGeneratorException extends StoreFailureException
{
    public InvalidIdGeneratorException( String msg )
    {
        super( msg );
    }

    public InvalidIdGeneratorException( Throwable cause )
    {
        super( cause );
    }

    public InvalidIdGeneratorException( String msg, Throwable cause )
    {
        super( msg, cause );
    }
}
