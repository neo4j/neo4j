package org.neo4j.util.shell;

import java.net.MalformedURLException;
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
 * Consists of host, port and name, as in. rmi://<host>:<port>/<name>
 */
public class RmiLocation
{
	private static String defaultHost;
	private static int defaultPort;
	private static String defaultName;
	
	private String host;
	private int port;
	private String name;
	
	public static void setDefaultHost( String host )
	{
		defaultHost = host;
	}
	
	public static void setDefaultport( int port )
	{
		defaultPort = port;
	}

	public static void setDefaultName( String name )
	{
		defaultName = name;
	}
	
	private RmiLocation()
	{
	}

	private RmiLocation( String host, int port, String name )
	{
		this.host = host;
		this.port = port;
		this.name = name;
	}
	
	public static RmiLocation location( String url )
	{
		// Parse the url
		int protocolIndex = url.indexOf( "://" );
		int portIndex = url.lastIndexOf( ':' );
		int nameIndex = url.indexOf( "/", portIndex );
		String host = url.substring( protocolIndex + 3, portIndex );
		int port = Integer.parseInt( url.substring( portIndex + 1,
			nameIndex ) );
		String name = url.substring( nameIndex + 1 );
		return location( host, port, name );
	}
	
	public static RmiLocation location( String host, int port, String name )
	{
		return new RmiLocation( host, port, name );
	}
	
	public static RmiLocation location( Map<String, Object> data )
	{
		String host = ( String ) data.get( "host" );
		Integer port = ( Integer ) data.get( "port" );
		String name = ( String ) data.get( "name" );

		return location(
			host == null ? defaultHost : host,
			port == null ? defaultPort : port,
			name == null ? defaultName : name );
	}
	
	public String getHost()
	{
		return this.host;
	}
	
	public int getPort()
	{
		return this.port;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	private static String getProtocol()
	{
		return "rmi://";
	}
	
	public String toShortUrl()
	{
		return getProtocol() + getHost() + ":" + getPort();
	}
	
	public String toUrl()
	{
		return getProtocol() + getHost() + ":" + getPort() + "/" + getName();
	}
	
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
	
	public Registry ensureRegistryCreated()
		throws RemoteException
	{
		try
		{
			Naming.list( toShortUrl() );
			return LocateRegistry.getRegistry( getPort() );
		}
		catch ( RemoteException e )
		{
			return LocateRegistry.createRegistry( getPort() );
		}
		catch ( java.net.MalformedURLException e )
		{
			throw new RemoteException( "Malformed URL", e );
		}
	}
	
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
