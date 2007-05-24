package org.neo4j.impl.command;

public class ExecuteFailedException extends Exception
{
	public ExecuteFailedException()
	{
		super();
	}
	
	public ExecuteFailedException( String message )
	{
		super( message );
	}

	public ExecuteFailedException( String message, Throwable cause)
	{
		super( message, cause );
	}

	public ExecuteFailedException( Throwable cause )
	{
		super( cause );
	}
	
}