package org.neo4j.util.shell;

import java.lang.reflect.Modifier;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;

/**
 * A common implementation of an {@link AppShellServer}. The server can be given
 * one or more java packages f.ex. "org.neo4j.util.shell.apps" where some
 * common apps exist. All classes in those packages which implements the
 * {@link App} interface will be available to execute.
 */
public abstract class AbstractAppServer extends AbstractServer
	implements AppShellServer
{
	private Set<String> packages = new HashSet<String>();

	/**
	 * Constructs a new server.
	 * @throws RemoteException if there's an RMI error.
	 */
	public AbstractAppServer()
		throws RemoteException
	{
		super();
	}

	/**
	 * Adds a package to scan for apps.
	 * @param pkg the java package, f.ex. "org.neo4j.util.shell.apps".
	 */
	public void addPackage( String pkg )
	{
		this.packages.add( pkg );
	}
	
	/**
	 * @return packages added by {@link #addPackage(String)}.
	 */
	public Set<String> getPackages()
	{
		return new HashSet<String>( packages );
	}

	public App findApp( String command )
	{
		for ( String pkg : this.packages )
		{
			String name = pkg + "." +
				command.substring( 0, 1 ).toUpperCase() +
				command.substring( 1, command.length() ).toLowerCase();
			try
			{
				Class<?> cls = Class.forName( name );
				if ( !cls.isInterface() && App.class.isAssignableFrom( cls ) &&
					Modifier.isAbstract( cls.getModifiers() ) )
				{
					App theApp = ( App ) cls.newInstance();
					theApp.setServer( this );
					return theApp;
				}
			}
			catch ( Exception e )
			{
			}
		}
		return null;
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException
	{
		if ( line == null || line.trim().length() == 0 )
		{
			return "";
		}
		
		AppCommandParser parser = new AppCommandParser( this, line );
		return parser.app().execute( parser, session, out );
	}
}
