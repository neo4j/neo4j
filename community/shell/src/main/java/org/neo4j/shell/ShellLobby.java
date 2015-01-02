/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.shell;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.shell.impl.SimpleAppServer;
import org.neo4j.shell.impl.RemoteClient;
import org.neo4j.shell.impl.RmiLocation;
import org.neo4j.shell.impl.SameJvmClient;

/**
 * A convenience class for creating servers clients as well as finding remote
 * servers.
 */
public abstract class ShellLobby
{
    public static final Map<String, Serializable> NO_INITIAL_SESSION = Collections.unmodifiableMap(
            Collections.<String,Serializable>emptyMap() );
    
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
	public static ShellClient newClient( ShellServer server ) throws ShellException
    {
	    return newClient( server, new HashMap<String, Serializable>() );
	}
	
    /**
     * Creates a client and "starts" it, i.e. grabs the console prompt.
     * @param server the server (in the same JVM) which the client will
     * communicate with.
     * @param initialSession the initial session variables the shell will have,
     * in addition to those provided by the server initially.
     * @return the new shell client.
     */
    public static ShellClient newClient( ShellServer server, Map<String, Serializable> initialSession ) throws ShellException
    {
        return new SameJvmClient( initialSession, server );
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
        return newClient( "localhost", port, name );
    }
    
    /**
     * Creates a client and "starts" it, i.e. grabs the console prompt.
     * It will try to find a remote server on "localhost" and default RMI name.
     * @param port the RMI port.
     * @throws ShellException if no server was found at the RMI location.
     * @return the new shell client.
     */
    public static ShellClient newClient( int port )
            throws ShellException
    {
        return newClient( "localhost", port );
    }
    
	/**
	 * Creates a client and "starts" it, i.e. grabs the console prompt.
	 * It will try to find a remote server to connect to.
     * @param host the host (IP or domain name).
	 * @param port the RMI port.
	 * @param name the RMI name.
	 * @throws ShellException if no server was found at the RMI location.
	 * @return the new shell client.
	 */
	public static ShellClient newClient( String host, int port, String name )
		throws ShellException
	{
		return newClient( RmiLocation.location( host, port, name ) );
	}

    /**
     * Creates a client and "starts" it, i.e. grabs the console prompt.
     * It will try to find a remote server to connect to. Uses default RMI name.
     * @param host the host (IP or domain name).
     * @param port the RMI port.
     * @throws ShellException if no server was found at the RMI location.
     * @return the new shell client.
     */
    public static ShellClient newClient( String host, int port )
        throws ShellException
    {
        return newClient( host, port, SimpleAppServer.DEFAULT_NAME );
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
		return newClient( serverLocation, new HashMap<String, Serializable>() );
	}
	
    /**
     * Creates a client and "starts" it, i.e. grabs the console prompt.
     * It will try to find a remote server specified by {@code serverLocation}.
     * @param serverLocation the RMI location of the server to connect to.
     * @param initialSession the initial session variables the shell will have,
     * in addition to those provided by the server initially.
     * @throws ShellException if no server was found at the RMI location.
     * @return the new shell client.
     */
    public static ShellClient newClient( RmiLocation serverLocation, Map<String, Serializable> initialSession )
        throws ShellException
    {
        return new RemoteClient( initialSession, serverLocation );
    }
    
    /**
     * Creates a client and "starts" it, i.e. grabs the console prompt.
     * It will try to find a remote server on {@code host} with default
     * port and name.
     * @param host host to connect to.
     * @throws ShellException if no server was found at the RMI location.
     * @return the new shell client.
     */
	public static ShellClient newClient( String host ) throws ShellException
	{
	    return newClient( host, SimpleAppServer.DEFAULT_PORT, SimpleAppServer.DEFAULT_NAME );
	}
	
    /**
     * Creates a client and "starts" it, i.e. grabs the console prompt.
     * It will try to find a remote server on localhost with default
     * port and name.
     * @throws ShellException if no server was found at the RMI location.
     * @return the new shell client.
     */
	public static ShellClient newClient() throws ShellException
	{
        return newClient( "localhost", SimpleAppServer.DEFAULT_PORT, SimpleAppServer.DEFAULT_NAME );
	}
	
    public static RmiLocation remoteLocation()
    {
        return remoteLocation( SimpleAppServer.DEFAULT_PORT );
    }
    
	public static RmiLocation remoteLocation( int port )
	{
	    return remoteLocation( port, SimpleAppServer.DEFAULT_NAME );
	}

    public static RmiLocation remoteLocation( int port, String rmiName )
    {
        return RmiLocation.location( "localhost", port, rmiName );
    }
}
