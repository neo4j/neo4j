package org.neo4j.util.shell.apps;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.util.shell.AbstractApp;
import org.neo4j.util.shell.AbstractAppServer;
import org.neo4j.util.shell.App;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.ClassLister;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;
import org.neo4j.util.shell.ShellServer;

public class Help extends AbstractApp
{
	private static Collection<String> availableCommands;
	
	@Override
	public String getDescription()
	{
		return "Lists available commands";
	}
	
	public String execute( AppCommandParser parser, Session session,
		Output out ) throws ShellException
	{
		try
		{
			out.println( getHelpString( getServer() ) );
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
		return null;
	}
	
	public static String getHelpString( ShellServer server )
	{
		return "Available commands: " +
			availableCommandsAsString( server ) + "\n" +
			"Use man <command> for info about each command.";
	}
	
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
			availableCommands.add( "quit" );
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
