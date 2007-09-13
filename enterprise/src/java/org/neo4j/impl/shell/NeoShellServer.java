package org.neo4j.impl.shell;

import java.rmi.RemoteException;
import org.neo4j.api.core.EmbeddedNeo;
//import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.shell.apps.Ls;
import org.neo4j.util.shell.AbstractClient;
import org.neo4j.util.shell.SimpleAppServer;

public class NeoShellServer extends SimpleAppServer
{
	private EmbeddedNeo neo;
	
	public NeoShellServer( EmbeddedNeo neo ) 
		throws RemoteException
	{
		super();
		this.addPackage( Ls.class.getPackage().getName() );
		this.neo = neo;
		this.setProperty( AbstractClient.PROMPT_KEY, "neo-sh$ " );
	}
	
	@Override
	public String welcome()
	{
		return
			"Welcome to NeoShell\n" +
			"Available commands: " +
			"cd env exit export gsh ls man mkrel mv pwd rm rmrel set quit\n" +
			"Use man <command> for info about each command.";
	}
	
	public EmbeddedNeo getNeo()
	{
		return this.neo;
	}	
}
