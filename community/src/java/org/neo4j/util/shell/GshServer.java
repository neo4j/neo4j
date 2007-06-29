package org.neo4j.util.shell;

import java.rmi.RemoteException;
import org.neo4j.util.shell.apps.extra.GshExecutor;

public class GshServer extends AbstractServer
{
	public GshServer() throws RemoteException
	{
		super();
		this.setProperty( AbstractClient.PROMPT_KEY, "gsh$ " );
	}

	public String interpretLine( String line, Session session, Output out )
		throws ShellException, RemoteException
	{
		session.set( AbstractClient.STACKTRACES_KEY, true );
		if ( line != null && ( line.equalsIgnoreCase( "exit" ) ||
			line.equalsIgnoreCase( "quit" ) ) )
		{
			return "e";
		}
		
		GshExecutor gsh = new GshExecutor();
		gsh.execute( line, session, out );
		return null;
	}
}
