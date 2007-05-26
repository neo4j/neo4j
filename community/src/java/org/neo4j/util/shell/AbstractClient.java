package org.neo4j.util.shell;

import java.rmi.RemoteException;

public abstract class AbstractClient implements ShellClient
{
	public static final String PROMPT_KEY = "PS1";
	public static final String STACKTRACES_KEY = "STACKTRACES";
	
	private Console console = new Console();
	
	public void grabPrompt()
	{
		this.init();
		while ( true )
		{
			try
			{
				this.console.format(
					( String ) this.session().get( PROMPT_KEY ) );
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
	
	protected void shutdown()
	{
	}
	
	private boolean shouldPrintStackTraces()
	{
		try
		{
			String value = ( String ) this.session().get( STACKTRACES_KEY );
			return this.getSafeBooleanValue( value, false );
		}
		catch ( RemoteException e )
		{
			return true;
		}
	}
	
	private boolean getSafeBooleanValue( String string, boolean def )
	{
		if ( string == null || string.trim().length() == 0 )
		{
			return def;
		}
		return Boolean.parseBoolean( string );
	}
	
	private void init()
	{
		try
		{
			if ( this.session().get( PROMPT_KEY ) == null )
			{
				String fromServer =
					( String ) this.getServer().getProperty( PROMPT_KEY );
				this.session().set( PROMPT_KEY, fromServer != null ?
					fromServer : "# " );
			}
			this.getOutput().println( this.getServer().welcome() );
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
