package org.neo4j.util.shell;

public class ShellException extends Exception
{
	public ShellException()
	{
	}

	public ShellException( String message )
	{
		super( message );
	}

	public ShellException( Throwable cause )
	{
		super( cause );
	}

	public ShellException( String message, Throwable cause )
	{
		super( message, cause );
	}
}
