package org.neo4j.impl.shell;

import java.rmi.RemoteException;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.shell.apps.Ls;
import org.neo4j.util.shell.AbstractClient;
import org.neo4j.util.shell.SimpleServer;

public class NeoShellServer extends SimpleServer
{
	private EmbeddedNeo neo;
	private Class<? extends RelationshipType> relTypeClass;
	
	public NeoShellServer( EmbeddedNeo neo,
		Class<? extends RelationshipType> relTypeClass ) throws RemoteException
	{
		super();
		this.addPackage( Ls.class.getPackage().getName() );
		this.neo = neo;
		this.relTypeClass = relTypeClass;
		this.setProperty( AbstractClient.PROMPT_KEY, "neo-sh$ " );
	}
	
	public NeoShellServer( EmbeddedNeo neo ) throws RemoteException
	{
		this( neo, neo.getRelationshipTypes() );
	}

	@Override
	public String welcome()
	{
		return
			"Welcome to NeoShell\n" +
			"Available commands: " +
			"cd env exit export gsh ls man mkrel mv pwd rm rmel set quit\n" +
			"Use man <command> for info about each command.";
	}
	
	public EmbeddedNeo getNeo()
	{
		return this.neo;
	}
	
	public Class<? extends RelationshipType> getRelationshipTypeClass()
	{
		return this.relTypeClass;
	}
}
