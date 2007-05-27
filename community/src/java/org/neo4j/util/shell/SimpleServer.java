package org.neo4j.util.shell;

import java.rmi.RemoteException;
import org.neo4j.util.shell.apps.Man;

public class SimpleServer extends AbstractServer
{
	public SimpleServer() throws RemoteException
	{
		super();
		this.addPackage( Man.class.getPackage().getName() );
	}
	
	private App findBuiltInApp( String command )
	{
		if ( command.equals( "exit" ) || command.equals( "quit" ) )
		{
			return new ExitApp();
		}
		return null;
	}
	
	@Override
	public App findApp( String command ) throws RemoteException
	{
		App app = this.findBuiltInApp( command );
		return app != null ? app : super.findApp( command );
	}

	private abstract class BuiltInApp extends AbstractApp
	{
	}
	
	private class ExitApp extends BuiltInApp
	{
		@Override
		public String getName()
		{
			return "exit";
		}
		
		public String execute( CommandParser parser, Session session,
			Output out ) throws ShellException
		{
			return "e";
		}
		
		@Override
		public String getDescription()
		{
			return "Built-in command. Exits the client";
		}
	}
}
