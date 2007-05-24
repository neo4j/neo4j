package org.neo4j.impl.transaction;

public class IllegalResourceException extends Exception
{
	public IllegalResourceException()
	{
		super();
	}

	public IllegalResourceException(String message)
	{
		super( message );
	}

	public IllegalResourceException(String message, Throwable cause)
	{
		super( message, cause );
	}

	public IllegalResourceException(Throwable cause)
	{
		super( cause );
	}
}

