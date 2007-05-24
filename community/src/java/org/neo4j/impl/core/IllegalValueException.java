package org.neo4j.impl.core;


public class IllegalValueException extends RuntimeException
{
	public IllegalValueException()
	{
		super();
	}
	
	public IllegalValueException( String message )
	{
		super( message );
	}

	public IllegalValueException( String message, Throwable cause)
	{
		super( message, cause );
	}

	public IllegalValueException( Throwable cause )
	{
		super( cause );
	}
	
}