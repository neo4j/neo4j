package org.neo4j.util.shell.apps;

import java.rmi.RemoteException;
import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.App;
import org.neo4j.util.shell.CommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Man extends AbstractApp
{
	public String execute( CommandParser parser, Session session, Output out )
		throws ShellException
	{
		if ( parser.arguments().size() == 0 )
		{
			throw new ShellException( "Need to supply an app: man <app>" );
		}
		
		App app = this.getApp( parser );
		try
		{
			out.println( "" );
			out.println( this.fixDesciption( app.getDescription() ) );
			println( out, "" );
			for ( String option : app.getAvailableOptions() )
			{
				String description = this.fixDesciption(
					app.getDescription( option ) );
				OptionValueType type = app.getOptionValueType( option );
				println( out, "-" + option + "\t" + description + " " +
				( type == OptionValueType.NONE ? "(no value)" : "" ) +
				( type == OptionValueType.MAY ? "(may have value)" : "" ) +
				( type == OptionValueType.MUST ? "(must have value)" : "" ) );
			}
			println( out, "" );
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
		return null;
	}
	
	private String fixDesciption( String description )
	{
		if ( description != null && !description.endsWith( "." ) )
		{
			description = description + ".";
		}
		return description;
	}
	
	private void println( Output out, String string ) throws RemoteException
	{
		out.println( "\t" + string );
	}

	private App getApp( CommandParser parser ) throws ShellException
	{
		String appName = parser.arguments().get( 0 );
		try
		{
			App app = this.getServer().findApp( appName );
			if ( app == null )
			{
				throw new ShellException( "No manual entry for '" +
					appName + "'" );
			}
			return app;
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
	}

	@Override
	public String getDescription()
	{
		return "Display a manual. Usage: man <app>";
	}
}
