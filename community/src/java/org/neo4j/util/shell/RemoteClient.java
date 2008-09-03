package org.neo4j.util.shell;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * A {@link ShellClient} implementation which uses a remote server,
 * where the output and session are remote also.
 */
public class RemoteClient extends AbstractClient
{
	private ShellServer server;
	private RmiLocation serverLocation;
	private Session session;
	private Output out;
	
	/**
	 * @param serverLocation the RMI location of the server to connect to.
	 * @throws ShellException if no server was found at the RMI location.
	 */
	public RemoteClient( RmiLocation serverLocation ) throws ShellException
	{
		this.serverLocation = serverLocation;
		this.server = findRemoteServer();
		this.session = RemoteSession.newSession();
		this.out = RemoteOutput.newOutput();
	}
	
	private ShellServer findRemoteServer() throws ShellException
	{
		try
		{
			return ( ShellServer ) this.serverLocation.getBoundObject();
		}
		catch ( RemoteException e )
		{
			throw new ShellException( e );
		}
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
		boolean shouldTryToReconnect = this.server == null;
		try
		{
			if ( !shouldTryToReconnect )
			{
				this.server.welcome();
			}
		}
		catch ( RemoteException e )
		{
			shouldTryToReconnect = true;
		}
		
		Exception originException = null;
		if ( shouldTryToReconnect )
		{
			this.server = null;
			try
			{
				this.server = findRemoteServer();
				getOutput().println( "[Reconnected to server]" );
			}
			catch ( ShellException ee )
			{
				// Ok
				originException = ee;
			}
			catch ( RemoteException ee )
			{
				// Ok
				originException = ee;
			}
		}
		
		if ( this.server == null )
		{
			throw new RuntimeException(
				"Server closed or cannot be reached anymore: " +
				originException.getMessage(), originException );
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
