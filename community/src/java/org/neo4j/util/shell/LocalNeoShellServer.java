package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Set;

public class LocalNeoShellServer extends SimpleAppServer
{
	private String neoDirectory;
	private Object neoServiceInstance;
	private SimpleAppServer neoServer;
	
	public LocalNeoShellServer( String neoDirectory ) throws RemoteException
	{
		super();
		this.neoDirectory = neoDirectory;
	}
	
	public LocalNeoShellServer( Object neoServiceInstance )
	    throws RemoteException
	{
	    super();
	    this.neoServiceInstance = neoServiceInstance;
	}
	
	private SimpleAppServer getNeoServer()
	{
		if ( this.neoServer == null )
		{
			try
			{
				this.neoServer = this.instantiateNewNeoServer();
			}
			catch ( ShellException e )
			{
				throw new RuntimeException( e.getMessage(), e );
			}
		}
		return this.neoServer;
	}
	
	private void shutdownServer()
	{
		if ( this.neoServer == null )
		{
			return;
		}
		
		try
		{
			this.neoServer.getClass().getMethod( "shutdown" ).invoke(
			    this.neoServer );
			if ( this.neoDirectory != null )
			{
			    this.neoServiceInstance.getClass().getMethod(
			        "shutdown" ).invoke( this.neoServiceInstance );
			}
			this.neoServiceInstance = null;
			this.neoServer = null;
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
			if ( this.neoServiceInstance == null )
			{
			    this.neoServiceInstance = neoClass.getConstructor(
			        String.class ).newInstance( this.neoDirectory );
			}
			Object neoShellServerObject = neoShellServerClass.getConstructor(
				neoServiceClass ).newInstance( neoServiceInstance );
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
		return this.getNeoServer().getName();
	}
	
	@Override
	public Serializable getProperty( String key )
	{
		return this.getNeoServer().getProperty( key );
	}
	
	@Override
	public void setProperty( String key, Serializable value )
	{
		this.getNeoServer().setProperty( key, value );
	}
	
	@Override
	public Set<String> getPackages()
	{
		return this.getNeoServer().getPackages();
	}
	
	@Override
	public App findApp( String command )
	{
		return this.getNeoServer().findApp( command );
	}
	
	@Override
	public String welcome()
	{
		return this.getNeoServer().welcome();
	}
	
	@Override
	public Serializable interpretVariable( String key, Serializable value,
		Session session ) throws RemoteException
	{
		return this.getNeoServer().interpretVariable( key, value, session );
	}
	
	@Override
	public void shutdown()
	{
		super.shutdown();
		if ( this.neoServer != null )
		{
	        this.shutdownServer();
		}
	}
}
