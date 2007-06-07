package org.neo4j.util.shell;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractAppServer extends AbstractServer
	implements AppShellServer
{
	private Set<String> packages = new HashSet<String>();

	public AbstractAppServer()
		throws RemoteException
	{
		super();
	}

	public void addPackage( String pkg )
	{
		this.packages.add( pkg );
	}

	public App findApp( String command ) throws RemoteException
	{
		for ( String pkg : this.packages )
		{
			String name = pkg + "." +
				command.substring( 0, 1 ).toUpperCase() +
				command.substring( 1, command.length() ).toLowerCase();
			try
			{
				App theApp = ( App ) Class.forName( name ).newInstance();
				theApp.setServer( this );
				return theApp;
			}
			catch ( Exception e )
			{
			}
		}
		return null;
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException
	{
		if ( line == null || line.trim().length() == 0 )
		{
			return "";
		}
		
		AppCommandParser parser = new AppCommandParser( this, line );
		return parser.app().execute( parser, session, out );
	}
}
