package org.neo4j.impl.transaction;

public class NotInTransactionException extends RuntimeException
{
	public NotInTransactionException()
	{
		super();
	}
	
	public NotInTransactionException( String message )
	{
		super( message );
	}

	public NotInTransactionException( String message, Throwable cause)
	{
		super( message, cause );
	}

	public NotInTransactionException( Throwable cause )
	{
		super( cause );
	}
}