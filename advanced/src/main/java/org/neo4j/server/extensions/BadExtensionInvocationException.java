package org.neo4j.server.extensions;

public final class BadExtensionInvocationException extends Exception
{
    public BadExtensionInvocationException( Throwable cause )
    {
        super( cause );
    }
}
