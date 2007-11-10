package org.neo4j.util.shell;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteClient extends AbstractClient
{
	private ShellServer server;
	private Session session;
	private Output out;
	
	public RemoteClient( ShellServer server )
	{
		this.server = server;
		this.session = RemoteSession.newSession();
		this.out = RemoteOutput.newOutput();
	}
	
	public Output getOutput()
	{
		return this.out;
	}

	public ShellServer getServer()
	{
		// Poke the server by calling a method, f.ex. the welcome() method.
		// If the connection is lost then try to reconnect, using the last
		// server lookup address.
		try
		{
			this.server.welcome();
		}
		catch ( RemoteException e )
		{
			RmiLocation lastLookup =
				ShellLobby.getInstance().getLastServerLookup();
			if ( lastLookup != null )
			{
				try
				{
					this.server = ShellLobby.getInstance().findRemoteServer(
						lastLookup );
					getOutput().println( "[Reconnected to server]" );
				}
				catch ( ShellException ee )
				{
					// Ok
				}
				catch ( RemoteException ee )
				{
					// Ok
				}
			}
		}
		return this.server;
	}

	public Session session()
	{
		return this.session;
	}
	
	@Override
	protected void shutdown()
	{
		this.tryUnexport( this.out );
		this.tryUnexport( this.session );
	}
	
	private void tryUnexport( Remote remote )
	{
		try
		{
			UnicastRemoteObject.unexportObject( remote, true );
		}
		catch ( NoSuchObjectException e )
		{
			System.out.println( "Couldn't unexport:" + remote );
		}
	}
}
