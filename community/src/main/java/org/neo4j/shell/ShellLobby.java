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
package org.neo4j.shell;

/**
 * A convenience class for creating servers clients as well as finding remote
 * servers.
 */
public abstract class ShellLobby
{
	/**
	 * To get rid of the RemoteException, uses a constructor without arguments.
	 * @param cls the class of the server to instantiate.
	 * @throws ShellException if the object couldn't be instantiated.
	 * @return a new shell server.
	 */
	public static ShellServer newServer( Class<? extends ShellServer> cls )
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

	/**
	 * Creates a client and "starts" it, i.e. grabs the console prompt.
	 * @param server the server (in the same JVM) which the client will
	 * communicate with.
	 * @return the new shell client.
	 */
	public static ShellClient newClient( ShellServer server )
	{
		return new SameJvmClient( server );
	}
	
	/**
	 * Creates a client and "starts" it, i.e. grabs the console prompt.
	 * It will try to find a remote server on "localhost".
	 * @param port the RMI port.
	 * @param name the RMI name.
	 * @throws ShellException if no server was found at the RMI location.
	 * @return the new shell client.
	 */
	public static ShellClient newClient( int port, String name )
		throws ShellException
	{
		return newClient( RmiLocation.location( "localhost", port, name ) );
	}

	/**
	 * Creates a client and "starts" it, i.e. grabs the console prompt.
	 * It will try to find a remote server specified by {@code serverLocation}.
	 * @param serverLocation the RMI location of the server to connect to.
	 * @throws ShellException if no server was found at the RMI location.
	 * @return the new shell client.
	 */
	public static ShellClient newClient( RmiLocation serverLocation )
		throws ShellException
	{
		return new RemoteClient( serverLocation );
	}
}
