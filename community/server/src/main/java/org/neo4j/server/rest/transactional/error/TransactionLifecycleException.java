package org.neo4j.server.rest.transactional.error;

public abstract class TransactionLifecycleException extends RuntimeException
{
    protected TransactionLifecycleException( String message )
    {
        super( message );
    }

    protected TransactionLifecycleException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public abstract Neo4jError toNeo4jError();
}
