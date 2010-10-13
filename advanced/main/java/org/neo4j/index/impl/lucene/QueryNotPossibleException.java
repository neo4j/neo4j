package org.neo4j.index.impl.lucene;

public class QueryNotPossibleException extends RuntimeException
{
    public QueryNotPossibleException()
    {
        super();
    }

    public QueryNotPossibleException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public QueryNotPossibleException( String message )
    {
        super( message );
    }

    public QueryNotPossibleException( Throwable cause )
    {
        super( cause );
    }
}
