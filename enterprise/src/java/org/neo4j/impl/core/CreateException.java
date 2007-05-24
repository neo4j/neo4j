package org.neo4j.impl.core;


public class CreateException extends RuntimeException
{
	public CreateException()
	{
		super();
	}
	
	public CreateException( String message )
	{
		super( message );
	}

	public CreateException( String message, Throwable cause)
	{
		super( message, cause );
	}

	public CreateException( Throwable cause )
	{
		super( cause );
	}
	
}