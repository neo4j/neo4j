/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A common implementation of a {@link ShellServer}.
 */
public abstract class AbstractServer extends UnicastRemoteObject
	implements ShellServer
{
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
	
	public void shutdown()
	{
		try
		{
			unexportObject( this, true );
		}
		catch ( NoSuchObjectException e )
		{
			// Ok
//			System.out.println( "Couldn't shutdown server" );
		}
	}

	public void makeRemotelyAvailable( int port, String name )
		throws RemoteException
	{
		RmiLocation location =
			RmiLocation.location( "localhost", port, name );
		location.bind( this );
	}
	
	public Iterable<String> getAllAvailableCommands()
	{
		return Collections.emptyList();
	}
}
