package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.RemoteException;

/**
 * A common implementation of a {@link ShellClient}.
 */
public abstract class AbstractClient implements ShellClient
{
	/**
	 * The session key for the prompt key, just like in Bash.
	 */
	public static final String PROMPT_KEY = "PS1";
	
	/**
	 * The session key for whether or not to print stack traces for exceptions. 
	 */
	public static final String STACKTRACES_KEY = "STACKTRACES";
	
	private Console console;
	
	public void grabPrompt()
	{
		this.init();
		while ( true )
		{
			try
			{
				this.console.format( tryGetProperPromptString() );
				String line = this.readLine();
				String result = this.getServer().interpretLine(
					line, this.session(), this.getOutput() );
				if ( result == null || result.trim().length() == 0 )
				{
					continue;
				}
				if ( result.contains( "e" ) )
				{
					break;
				}
			}
			catch ( Exception e )
			{
				if ( this.shouldPrintStackTraces() )
				{
					e.printStackTrace();
				}
				this.console.format( e.getMessage() + "\n" );
			}
		}
		this.shutdown();
	}
	
	protected String tryGetProperPromptString()
	{
		String result = null;
		try
		{
			result = ( String ) getSessionVariable( PROMPT_KEY, null, true );
		}
		catch ( Exception e )
		{
			result = ( String ) getSessionVariable( PROMPT_KEY, null, false );
		}
		return result;
	}
	
	protected void shutdown()
	{
	}
	
	private boolean shouldPrintStackTraces()
	{
		try
		{
			Object value = this.session().get( STACKTRACES_KEY );
			return this.getSafeBooleanValue( value, false );
		}
		catch ( RemoteException e )
		{
			return true;
		}
	}
	
	private boolean getSafeBooleanValue( Object value, boolean def )
	{
		if ( value == null )
		{
			return def;
		}
		return Boolean.parseBoolean( value.toString() );
	}
	
	private void init()
	{
		try
		{
			possiblyGrabDefaultVariableFromServer( PROMPT_KEY, "$ " );
			this.getOutput().println( this.getServer().welcome() );
			
			// Grab a jline console if available, else a standard one.
			this.console = JLineConsole.newConsoleOrNullIfNotFound( this );
			if ( this.console == null )
			{
				System.out.println( "Want bash-like features? throw in " +
					"jLine on the classpath" );
				this.console = new StandardConsole();
			}
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	protected void possiblyGrabDefaultVariableFromServer( String key,
		Serializable defaultValue )
	{
		try
		{
			if ( this.session().get( key ) == null )
			{
				Serializable value = this.getServer().getProperty( key );
				if ( value == null )
				{
					value = defaultValue;
				}
				if ( value != null )
				{
					this.session().set( key, value );
				}
			}
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	protected Serializable getSessionVariable( String key,
		Serializable defaultValue, boolean interpret )
	{
		try
		{
			Serializable result = this.session().get( key );
			if ( result == null )
			{
				result = defaultValue;
			}
			if ( interpret && result != null )
			{
				result = this.getServer().interpretVariable( key, result,
					session() );
			}
			return result;
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}

	public String readLine()
	{
		return this.console.readLine();
	}
}
