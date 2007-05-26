package org.neo4j.util.shell;

import java.rmi.RemoteException;

public class ShellLobby
{
	private static final ShellLobby INSTANCE = new ShellLobby();
	
	public static ShellLobby getInstance()
	{
		return INSTANCE;
	}
	
	private ShellLobby()
	{
	}
	
	/**
	 * To get rid of the RemoteException, uses a constructor without arguments
	 */
	public ShellServer newServer( Class<? extends ShellServer> cls )
		throws ShellException
	{
		try
		{
			return cls.newInstance();
		}
		catch ( Exception e )
		{
			throw new RuntimeException( e );
		}
	}

	public ShellServer findRemoteServer( RmiLocation location )
		throws ShellException
	{
		try
		{
			return ( ShellServer ) location.getBoundObject();
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	public ShellServer findRemoteServer( int port, String name )
		throws ShellException
	{
		return this.findRemoteServer(
			RmiLocation.location( "localhost", port, name ) );
	}
	
	public ShellClient startClient( ShellServer server )
	{
		return this.startClient( server, null );
	}

	/**
	 * Instantiates a ShellClient and grabs the prompt.
	 * @param server the server to use
	 * @param sameJvmOrNullForAuto <code>true</code> for a client which is
	 * optimized for use in the same JVM as the server. <code>false</code> for
	 * a client which handles a remote server and <code>null</code> for
	 * auto-detect.
	 * @return the new shell client
	 */
	public ShellClient startClient( ShellServer server,
		Boolean sameJvmOrNullForAuto )
	{
		ShellClient client = sameJvmOrNullForAuto == Boolean.TRUE ?
			new SameJvmClient( server ) : new RemoteClient( server );
		client.grabPrompt();
		return client;
	}
}
