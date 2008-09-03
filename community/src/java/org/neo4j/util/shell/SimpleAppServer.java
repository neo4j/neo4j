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
		return null;
	}
	
	@Override
	public App findApp( String command )
	{
		App app = this.findBuiltInApp( command );
		return app != null ? app : super.findApp( command );
	}
}
