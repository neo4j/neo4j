package org.neo4j.kernel.ha;

public class HaCommunicationException extends RuntimeException
{
    public HaCommunicationException()
    {
        super();
    }

    public HaCommunicationException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public HaCommunicationException( String message )
    {
        super( message );
    }

    public HaCommunicationException( Throwable cause )
    {
        super( cause );
    }
}
