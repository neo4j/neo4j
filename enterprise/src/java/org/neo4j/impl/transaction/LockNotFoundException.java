package org.neo4j.impl.transaction;

public class LockNotFoundException extends Exception
{
	public LockNotFoundException()
	{
		super();
	}
	
	public LockNotFoundException(String message)
	{
		super( message );
	}

	public LockNotFoundException(String message, Throwable cause)
	{
		super( message, cause );
	}

	public LockNotFoundException(Throwable cause)
	{
		super( cause );
	}
}

