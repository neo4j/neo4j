package org.neo4j.util.shell;

import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
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
