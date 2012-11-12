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

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.TabCompletion;

/**
 * A common implementation of a {@link ShellServer}.
 */
public abstract class AbstractServer implements ShellServer
{
    private ShellServer remoteEndPoint;
    
	/**
	 * The default RMI name for a shell server,
	 * see {@link #makeRemotelyAvailable(int, String)}.
	 */
	public static final String DEFAULT_NAME = "shell";
	
	/**
	 * The default RMI port for a shell server,
	 * see {@link #makeRemotelyAvailable(int, String)}.
	 */
	public static final int DEFAULT_PORT = 1337;
	
	private Map<String, Serializable> properties =
		new HashMap<String, Serializable>();
	
	/**
	 * Constructs a new server.
	 * @throws RemoteException if an RMI exception occurs.
	 */
	public AbstractServer()
		throws RemoteException
	{
		super();
	}
	
	public String getName()
	{
		return DEFAULT_NAME;
	}

	public Serializable getProperty( String key )
	{
		return this.properties.get( key );
	}

	public void setProperty( String key, Serializable value )
	{
		this.properties.put( key, value );
	}
	
	public Serializable interpretVariable( String key, Serializable value,
		Session session ) throws ShellException, RemoteException
	{
		return session.get( key );
	}

	public String welcome()
	{
		return "Welcome to the shell";
	}
	
	public synchronized void shutdown() throws RemoteException
	{
	    if ( remoteEndPoint != null )
	    {
	        remoteEndPoint.shutdown();
	        remoteEndPoint = null;
	    }
	}

	public synchronized void makeRemotelyAvailable( int port, String name )
		throws RemoteException
	{
	    if ( remoteEndPoint == null )
	        remoteEndPoint = new RemotelyAvailableServer( this );
	    remoteEndPoint.makeRemotelyAvailable( port, name );
	}
	
	public String[] getAllAvailableCommands()
	{
		return new String[0];
	}
	
	public TabCompletion tabComplete( String partOfLine, Session session )
	        throws ShellException, RemoteException
	{
	    return new TabCompletion( Collections.<String>emptyList(), 0 );
	}
}
