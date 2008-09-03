package org.neo4j.util.shell;

import java.rmi.RemoteException;
import org.neo4j.util.shell.apps.extra.GshExecutor;

/**
 * A common {@link ShellServer} implementation which is specialized in just
 * executing groovy scripts.
 */
public class GshServer extends AbstractServer
{
	/**
	 * Constructs a new groovy shell server.
	 * @throws RemoteException if an RMI exception occurs.
	 */
	public GshServer() throws RemoteException
	{
		super();
		this.setProperty( AbstractClient.PROMPT_KEY, "gsh$ " );
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException
	{
		session.set( AbstractClient.STACKTRACES_KEY, true );
		GshExecutor gsh = new GshExecutor();
		gsh.execute( line, session, out );
		return null;
	}
}
