package org.neo4j.util.shell.apps;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.AbstractAppServer;
import org.neo4j.util.shell.AbstractClient;
import org.neo4j.util.shell.App;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.AppShellServer;
import org.neo4j.util.shell.ClassLister;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;
import org.neo4j.util.shell.ShellServer;

/**
 * Prints a short manual for an {@link App}.
 */
public class Man extends AbstractApp
{
	private static Collection<String> availableCommands;

	public String execute( AppCommandParser parser, Session session,
		Output out ) throws ShellException
	{
		try
		{
			if ( parser.arguments().size() == 0 )
			{
				out.println( getHelpString( getServer() ) );
				return null;
			}
			
			App app = this.getApp( parser );
			out.println( "" );
			out.println( this.fixDesciption( app.getDescription() ) );
			println( out, "" );
			boolean hasOptions = false;
			for ( String option : app.getAvailableOptions() )
			{
				hasOptions = true;
				String description = this.fixDesciption(
					app.getDescription( option ) );
				OptionValueType type = app.getOptionValueType( option );
				println( out, "-" + option + "\t" + description + " " +
				( type == OptionValueType.NONE ? "" : "" ) +
				( type == OptionValueType.MAY ? "(may have value)" : "" ) +
				( type == OptionValueType.MUST ? "(must have value)" : "" ) );
			}
			if ( hasOptions )
			{
				println( out, "" );
			}
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
		return null;
	}
	
	private static String getShortUsageString()
	{
		return "man <command>";
	}
	
	private String fixDesciption( String description )
	{
		if ( description == null )
		{
			description = "";
		}
		else if ( !description.endsWith( "." ) )
		{
			description = description + ".";
		}
		return description;
	}
	
	private void println( Output out, String string ) throws RemoteException
	{
		out.println( "\t" + string );
	}

	private App getApp( AppCommandParser parser ) throws ShellException
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
		return "Display a manual for a command or a general help message.\n" +
			"Usage: " + getShortUsageString();
	}

	/**
	 * Utility method for getting a short help string for a server.
	 * Basically it contains an introductory message and also lists all
	 * available apps for the server.
	 * @param server the server to ask for 
	 * @return the short introductory help string.
	 */
	public static String getHelpString( ShellServer server )
	{
		return "Available commands: " +
			availableCommandsAsString( server ) + "\n" +
			"Use " + getShortUsageString() + " for info about each command.";
	}
	
	/**
	 * Uses {@link ClassLister} to list apps available at the server.
	 * @param server the {@link ShellServer}.
	 * @return a list of available commands a client can execute, whre the
	 * server is an {@link AppShellServer}.
	 */
	public static synchronized Collection<String> getAvailableCommands(
		ShellServer server )
	{
		if ( availableCommands == null )
		{
			// TODO Shouldn't trust the server to be an AbstractAppServer
			Set<String> packages =
				( ( AbstractAppServer ) server ).getPackages();
			availableCommands = new TreeSet<String>();
			for ( Class<? extends App> appClass :
				ClassLister.listClassesExtendingOrImplementing( App.class,
				packages ) )
			{
				if ( packages.contains( appClass.getPackage().getName() ) )
				{
					availableCommands.add(
						appClass.getSimpleName().toLowerCase() );
				}
			}
			for ( String exitCommand : AbstractClient.getExitCommands() )
			{
				availableCommands.add( exitCommand );
			}
		}
		return availableCommands;
	}
	
	private static synchronized String availableCommandsAsString(
		ShellServer server )
	{
		StringBuffer commands = new StringBuffer();
		for ( String command : getAvailableCommands( server ) )
		{
			if ( commands.length() > 0 )
			{
				commands.append( " " );
			}
			commands.append( command );
		}
		return commands.toString();
	}
}
