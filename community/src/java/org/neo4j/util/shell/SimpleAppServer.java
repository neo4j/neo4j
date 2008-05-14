package org.neo4j.util.shell;

import java.rmi.RemoteException;

import org.neo4j.util.shell.apps.Man;
import org.neo4j.util.shell.apps.extra.Gsh;

/**
 * A common concrete implement of an {@link AppShellServer} which contains
 * default packages and exit app.
 */
public class SimpleAppServer extends AbstractAppServer
{
	private static final String APP_EXIT = "exit";
	private static final String APP_QUIT = "quit";
	
	/**
	 * Creates a new simple app server and adds default packages.
	 * @throws RemoteException RMI error.
	 */
	public SimpleAppServer() throws RemoteException
	{
		super();
		this.addPackage( Man.class.getPackage().getName() );
	}
	
	protected void addExtraPackage()
	{
		this.addPackage( Gsh.class.getPackage().getName() );
	}
	
	private App findBuiltInApp( String command )
	{
		if ( command.equals( APP_EXIT ) || command.equals( APP_QUIT ) )
		{
			return new ExitApp();
		}
		return null;
	}
	
	@Override
	public App findApp( String command )
	{
		App app = this.findBuiltInApp( command );
		return app != null ? app : super.findApp( command );
	}
	
//	@Override
//	protected Set<String> findAllApps()
//	{
//		Set<String> set = super.findAllApps();
//		set.add( APP_EXIT );
//		set.add( APP_QUIT );
//		return set;
//	}

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
		
		public String execute( AppCommandParser parser, Session session,
			Output out ) throws ShellException
		{
			return "e";
		}
		
		@Override
		public String getDescription()
		{
			return "Exits the client";
		}
	}
}
