package org.neo4j.impl.shell;

import java.rmi.RemoteException;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.util.shell.AbstractServer;
import org.neo4j.util.shell.ShellException;
import org.neo4j.util.shell.ShellServer;

public class NeoShellLobby
{
	private static final NeoShellLobby INSTANCE = new NeoShellLobby();
	
	public static NeoShellLobby getInstance()
	{
		return INSTANCE;
	}
	
	private NeoShellLobby()
	{
	}
	
	public ShellServer startNeoShellServerWithNeo(
		Class<? extends RelationshipType> relTypes, String neoPath )
		throws ShellException
	{
		final EmbeddedNeo neo = new EmbeddedNeo( relTypes, neoPath );
		try
		{
			final ShellServer server = new NeoShellServer( neo ); //, relTypes );
			Runtime.getRuntime().addShutdownHook( new Thread()
			{
				@Override
				public void run()
				{
					try
					{
						server.shutdown();
					}
					catch ( RemoteException e )
					{
						e.printStackTrace();
					}
					neo.shutdown();
				}
			} );
			server.makeRemotelyAvailable( AbstractServer.DEFAULT_PORT,
				AbstractServer.DEFAULT_NAME );
			return server;
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
	}
}
