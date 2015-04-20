/*
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
package org.neo4j.shell.impl;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

/**
 * Class for specifying a location of an RMI object
 * Consists of host, port and name, as in. "rmi://<host>:<port>/<name>"
 */
public class RmiLocation
{
	private String host;
	private int port;
	private String name;
	
	private RmiLocation()
	{
	}

	private RmiLocation( String host, int port, String name )
	{
		this.host = host;
		this.port = port;
		this.name = name;
	}
	
	/**
	 * Creates a new RMI location instance.
	 * @param url the RMI URL.
	 * @return the {@link RmiLocation} instance for {@code url}.
	 */
	public static RmiLocation location( String url )
	{
		int protocolIndex = url.indexOf( "://" );
		int portIndex = url.lastIndexOf( ':' );
		int nameIndex = url.indexOf( "/", portIndex );
		String host = url.substring( protocolIndex + 3, portIndex );
		int port = Integer.parseInt( url.substring( portIndex + 1,
			nameIndex ) );
		String name = url.substring( nameIndex + 1 );
		return location( host, port, name );
	}
	
	/**
	 * Creates a new RMI location instance.
	 * @param host the RMI host, f.ex. "localhost".
	 * @param port the RMI port.
	 * @param name the RMI name, f.ex. "shell".
	 * @return a new {@link RmiLocation} instance.
	 */
	public static RmiLocation location( String host, int port, String name )
	{
		return new RmiLocation( host, port, name );
	}
	
	/**
	 * Creates a new RMI location instance.
	 * @param data a map with data, should contain "host", "port" and "name".
	 * See {@link #location(String, int, String)}.
	 * @return a new {@link RmiLocation} instance.
	 */
	public static RmiLocation location( Map<String, Object> data )
	{
		String host = ( String ) data.get( "host" );
		Integer port = ( Integer ) data.get( "port" );
		String name = ( String ) data.get( "name" );
		return location( host, port, name );
	}
	
	/**
	 * @return the host of this RMI location.
	 */
	public String getHost()
	{
		return this.host;
	}
	
	/**
	 * @return the port of this RMI location.
	 */
	public int getPort()
	{
		return this.port;
	}
	
	/**
	 * @return the name of this RMI location.
	 */
	public String getName()
	{
		return this.name;
	}
	
	private static String getProtocol()
	{
		return "rmi://";
	}
	
	/**
	 * @return "short" URL, consisting of protocol, host and port, f.ex.
	 * "rmi://localhost:8080".
	 */
	public String toShortUrl()
	{
		return getProtocol() + getHost() + ":" + getPort();
	}
	
	/**
	 * @return the full RMI URL, f.ex.
	 * "rmi://localhost:8080/the/shellname".
	 */
	public String toUrl()
	{
		return getProtocol() + getHost() + ":" + getPort() + "/" + getName();
	}
	
	/**
	 * @return whether or not the RMI registry is created in this JVM for the
	 * given port, see {@link #getPort()}.
	 */
	public boolean hasRegistry()
	{
		try
		{
			Naming.list( toShortUrl() );
			return true;
		}
		catch ( RemoteException e )
		{
			return false;
		}
		catch ( java.net.MalformedURLException e )
		{
			return false;
		}
	}
	
	/**
	 * Ensures that the RMI registry is created for this JVM instance and port,
	 * see {@link #getPort()}.
	 * @return the registry for the port. 
	 * @throws RemoteException RMI error.
	 */
	public Registry ensureRegistryCreated()
		throws RemoteException
	{
		try
		{
			Naming.list( toShortUrl() );
			return LocateRegistry.getRegistry( getHost(), getPort() );
		}
		catch ( RemoteException e )
		{
            try
            {
                return LocateRegistry.createRegistry( getPort(), null, new HostBoundSocketFactory( host ) );
            }
            catch ( UnknownHostException hostException )
            {
                throw new RemoteException( "Unable to bind to '"+host+"', unknown hostname.", hostException );
            }
        }
		catch ( java.net.MalformedURLException e )
		{
			throw new RemoteException( "Malformed URL", e );
		}
	}
	
	/**
	 * Binds an object to the RMI location defined by this instance.
	 * @param object the object to bind.
	 * @throws RemoteException RMI error.
	 */
	public void bind( Remote object ) throws RemoteException
	{
		ensureRegistryCreated();
		try
		{
			Naming.rebind( toUrl(), object );
		}
		catch ( MalformedURLException e )
		{
			throw new RemoteException( "Malformed URL", e );
		}
	}
	
	/**
	 * Unbinds an object from the RMI location defined by this instance.
	 * @param object the object to bind.
	 * @throws RemoteException RMI error.
	 */
	public void unbind( Remote object ) throws RemoteException
	{
		try
		{
			Naming.unbind( toUrl() );
			UnicastRemoteObject.unexportObject( object, true );
		}
		catch ( NotBoundException e )
		{
			throw new RemoteException( "Not bound", e );
		}
		catch ( MalformedURLException e )
		{
			throw new RemoteException( "Malformed URL", e );
		}
	}
	
	/**
	 * @return whether or not there's an object bound to the RMI location
	 * defined by this instance.
	 */
	public boolean isBound()
	{
		try
		{
			getBoundObject();
			return true;
		}
		catch ( RemoteException e )
		{
			return false;
		}
	}
	
	/**
	 * @return the bound object for the RMI location defined by this instance.
	 * @throws RemoteException if there's no object bound for the RMI location.
	 */
	public Remote getBoundObject() throws RemoteException
	{
		try
		{
			return Naming.lookup( toUrl() );
		}
		catch ( NotBoundException e )
		{
			throw new RemoteException( "Not bound", e );
		}
		catch ( MalformedURLException e )
		{
			throw new RemoteException( "Malformed URL", e );
		}
	}
}
