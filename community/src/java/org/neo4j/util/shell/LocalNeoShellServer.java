package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Set;

public class LocalNeoShellServer extends SimpleAppServer
{
	private String neoDirectory;
	private Object neoObject;
	private SimpleAppServer neoServer;
	
	public LocalNeoShellServer( String neoDirectory ) throws RemoteException
	{
		super();
		this.neoDirectory = neoDirectory;
	}
	
	private SimpleAppServer getNeoServer()
	{
		if ( neoServer == null )
		{
			try
			{
				neoServer = instantiateNewNeoServer();
			}
			catch ( ShellException e )
			{
				throw new RuntimeException( e.getMessage(), e );
			}
		}
		return neoServer;
	}
	
	private void shutdownServer()
	{
		if ( neoServer == null )
		{
			return;
		}
		
		try
		{
			neoServer.getClass().getMethod( "shutdown" ).invoke( neoServer );
			neoObject.getClass().getMethod( "shutdown" ).invoke( neoObject );
			neoObject = null;
			neoServer = null;
		}
		catch ( Exception e )
		{
			e.printStackTrace();
		}
	}
	
	private SimpleAppServer instantiateNewNeoServer() throws ShellException
	{
		String neoServiceClassName = "org.neo4j.api.core.NeoService";
		String neoClassName = "org.neo4j.api.core.EmbeddedNeo";
		String neoShellServerClassName = "org.neo4j.impl.shell.NeoShellServer";
		try
		{
			Class<?> neoClass = Class.forName( neoClassName );
			Class<?> neoServiceClass = Class.forName( neoServiceClassName );
			Class<?> neoShellServerClass =
				Class.forName( neoShellServerClassName );
			neoObject = neoClass.getConstructor(
				String.class ).newInstance( neoDirectory );
			Object neoShellServerObject = neoShellServerClass.getConstructor(
				neoServiceClass ).newInstance( neoObject );
			return ( SimpleAppServer ) neoShellServerObject;
		}
		catch ( Exception e )
		{
			throw new ShellException( e );
		}
	}
	
	@Override
	public String getName()
	{
		return getNeoServer().getName();
	}
	
	@Override
	public Serializable getProperty( String key )
	{
		return getNeoServer().getProperty( key );
	}
	
	@Override
	public void setProperty( String key, Serializable value )
	{
		getNeoServer().setProperty( key, value );
	}
	
	@Override
	public Set<String> getPackages()
	{
		return getNeoServer().getPackages();
	}
	
	@Override
	public App findApp( String command )
	{
		return getNeoServer().findApp( command );
	}
	
	@Override
	public String welcome()
	{
		return getNeoServer().welcome();
	}
	
	@Override
	public Serializable interpretVariable( String key, Serializable value,
		Session session ) throws RemoteException
	{
		return getNeoServer().interpretVariable( key, value, session );
	}
	
	@Override
	public void shutdown()
	{
		super.shutdown();
		if ( neoServer == null )
		{
			return;
		}
		shutdownServer();
	}
}
