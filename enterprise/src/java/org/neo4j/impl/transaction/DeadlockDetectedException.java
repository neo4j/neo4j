package org.neo4j.impl.transaction;

public class DeadlockDetectedException extends RuntimeException
{
	public DeadlockDetectedException()
	{
		super();
	}
	
	public DeadlockDetectedException(String message)
	{
		super( message );
	}

	public DeadlockDetectedException(String message, Throwable cause)
	{
		super( message, cause );
	}

	public DeadlockDetectedException(Throwable cause)
	{
		super( cause );
	}
}
