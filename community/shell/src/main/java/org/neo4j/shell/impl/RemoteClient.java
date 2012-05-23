/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.impl;

import java.rmi.RemoteException;

import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;

/**
 * A {@link ShellClient} implementation which uses a remote server,
 * where the output and session are remote also.
 */
public class RemoteClient extends AbstractClient
{
	private ShellServer server;
	private final RmiLocation serverLocation;
	private final Output out;
	private final SessionImpl session;

    public RemoteClient( RmiLocation serverLocation ) throws ShellException
    {
        this( serverLocation, RemoteOutput.newOutput() );
    }
    
	/**
	 * @param serverLocation the RMI location of the server to connect to.
	 * @throws ShellException if no server was found at the RMI location.
	 */
	public RemoteClient( RmiLocation serverLocation, Output out ) throws ShellException
	{
		this.serverLocation = serverLocation;
		this.server = findRemoteServer();
		this.out = out;
		this.session = new RemoteSession();
	}

	private ShellServer findRemoteServer() throws ShellException
	{
		try
		{
			ShellServer result = ( ShellServer ) this.serverLocation.getBoundObject();
			updateTimeForMostRecentConnection();
			return result;
		}
		catch ( RemoteException e )
		{
			throw ShellException.wrapCause( e );
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
				regrabVariablesFromServer( this.server );
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
	
	@Override
	public Session session()
	{
	    return session;
	}

	public void shutdown()
	{
	    super.shutdown();
        if ( session.writer != null ) tryUnexport( session.writer );
		tryUnexport( this.out );
	}
}
