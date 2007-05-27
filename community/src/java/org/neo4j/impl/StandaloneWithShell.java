package org.neo4j.impl;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.RelationshipType;

public class StandaloneWithShell
{
	private static Logger log = Logger.getLogger(
		StandaloneWithShell.class.getName() );	
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
		log.info( "Neo started" );
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
			log.info( "Shutting down..." );
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
				log.warning( "Error shutting down Neo: " +  t );
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
