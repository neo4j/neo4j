package org.neo4j.util.shell;

/**
 * A general shell exception when an error occurs.
 */
public class ShellException extends Exception
{
	/**
	 * Empty exception.
	 */
	public ShellException()
	{
	}

	/**
	 * @param message the description of the exception.
	 */
	public ShellException( String message )
	{
		super( message );
	}

	/**
	 * @param cause the cause of the exception.
	 */
	public ShellException( Throwable cause )
	{
		super( cause );
	}

	/**
	 * @param message the description of the exception.
	 * @param cause the cause of the exception.
	 */
	public ShellException( String message, Throwable cause )
	{
		super( message, cause );
	}
}
