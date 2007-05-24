package org.neo4j.impl.core;


public class DeleteException extends RuntimeException
{
	public DeleteException()
	{
		super();
	}
	
	public DeleteException( String message )
	{
		super( message );
	}

	public DeleteException( String message, Throwable cause)
	{
		super( message, cause );
	}

	public DeleteException( Throwable cause )
	{
		super( cause );
	}
	
}