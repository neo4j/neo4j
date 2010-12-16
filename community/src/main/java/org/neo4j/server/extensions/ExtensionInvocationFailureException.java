package org.neo4j.server.extensions;

public final class ExtensionInvocationFailureException extends Exception
{
    ExtensionInvocationFailureException( Throwable cause )
    {
        super( cause );
    }
}
