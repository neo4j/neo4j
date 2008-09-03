package org.neo4j.util.shell;

import java.rmi.RemoteException;

import org.neo4j.util.shell.apps.extra.JshExecutor;

/**
 * A common {@link ShellServer} implementation which is specialized in just
 * executing groovy scripts.
 */
public class JshServer extends AbstractServer
{
	/**
	 * Constructs a new groovy shell server.
	 * @throws RemoteException if an RMI exception occurs.
	 */
	public JshServer() throws RemoteException
	{
		super();
		this.setProperty( AbstractClient.PROMPT_KEY, "jsh$ " );
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException
	{
		session.set( AbstractClient.STACKTRACES_KEY, true );
		JshExecutor jsh = new JshExecutor();
		jsh.execute( line, session, out );
		return null;
	}
}
