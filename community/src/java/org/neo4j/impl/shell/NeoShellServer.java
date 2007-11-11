package org.neo4j.impl.shell;

import java.rmi.RemoteException;
import org.neo4j.api.core.NeoService;
import org.neo4j.impl.shell.apps.Ls;
import org.neo4j.util.shell.AbstractClient;
import org.neo4j.util.shell.SimpleAppServer;
import org.neo4j.util.shell.apps.Help;

public class NeoShellServer extends SimpleAppServer
{
	private NeoService neo;
	
	public NeoShellServer( NeoService neo ) 
		throws RemoteException
	{
		super();
		this.addPackage( Ls.class.getPackage().getName() );
		this.neo = neo;
		this.setProperty( AbstractClient.PROMPT_KEY, "neo-sh [0] $ " );
	}
	
	@Override
	public String welcome()
	{
		return "Welcome to NeoShell\n" +
			Help.getHelpString( this );
	}
	
	public NeoService getNeo()
	{
		return this.neo;
	}	
}
