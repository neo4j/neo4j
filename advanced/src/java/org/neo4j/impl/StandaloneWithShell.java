package org.neo4j.impl;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.api.core.*;

public class StandaloneWithShell
{
	private EmbeddedNeo embeddedNeo;
	private AtomicBoolean shutdownInitiated = new AtomicBoolean( false );
	
	private static enum RelTypes implements RelationshipType
	{
		A_RELATIONSHIP_TYPE,
	}
	
	private EmbeddedNeo getNeo()
	{
		return this.embeddedNeo;
	}
	
	private void addShutdownHook()
	{
		Runtime.getRuntime().addShutdownHook(
			new Thread()
			{
				public void run()
				{
					shutdown();
				}
			} );
	}
	
	private void initialize()
	{
		this.embeddedNeo = new EmbeddedNeo( RelTypes.class, "var" );
		Map<String, Serializable> shellProperties = Collections.emptyMap();
		getNeo().enableRemoteShell( shellProperties );
		System.out.println( "Neo started" );
	}
	
	private void blockUntilShutdown()
	{
		try
		{
			while ( shutdownInitiated.get() == false )
			{
				Thread.sleep( 100 );
			}
		}
		catch ( InterruptedException e )
		{
			// Exit
		}		
	}
	
	private void shutdown()
	{
		if ( shutdownInitiated.compareAndSet( false, true ) )
		{
			System.out.println( "Shutting down..." );
			try
			{
				if ( getNeo() != null )
				{
					getNeo().shutdown();			
					this.embeddedNeo = null;
				}
			}
			catch ( Throwable t )
			{
				System.err.println( "Error shutting down Neo: " +  t );
			}
		}		
	}

	public void execute()
	{
		addShutdownHook();
		initialize();
		blockUntilShutdown();
	}
	
	public static void main( String[] args )
	{
		new StandaloneWithShell().execute();
	}
}
