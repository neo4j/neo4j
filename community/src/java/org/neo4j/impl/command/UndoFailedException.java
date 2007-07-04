package org.neo4j.impl.command;

/**
 * This indicates a very serious error. No matter what happens a command
 * should always be able to undo. Throwing this kind of exception should
 * result in shutdown, and as you know... Neo never ever shuts down unless 
 * the user wants it to. Since the Neo is utterly perfect this 
 * exception is unnecessary but we keep it just for solidarity with other
 * coders.
 */
public class UndoFailedException extends RuntimeException
{
	public UndoFailedException()
	{
		super();
	}
	
	public UndoFailedException( String message )
	{
		super( message );
	}

	public UndoFailedException( String message, Throwable cause)
	{
		super( message, cause );
	}

	public UndoFailedException( Throwable cause )
	{
		super( cause );
	}
}