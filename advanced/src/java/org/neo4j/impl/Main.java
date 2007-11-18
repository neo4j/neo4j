package org.neo4j.impl;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.core.LockReleaser;
import org.neo4j.impl.transaction.LockManager;

public class Main
{
	private enum MyRelTypes implements RelationshipType
	{
	}
	
	private static NeoService neo;
	
	private static void startupNeo()
	{
		neo = new EmbeddedNeo( MyRelTypes.class, "var/nioneo" );
	}
	
	
	public static void main( String[] args )
	{
		startupNeo();
		Runtime.getRuntime().addShutdownHook( new ShutdownHook() );	
	}
	
	private static class ShutdownHook extends Thread
	{
		ShutdownHook()
		{
		}
		
		public void run()
		{
			// Then dump lock information
			try
			{
				LockReleaser.getManager().dumpLocks();
				LockManager.getManager().dumpRagStack();
				LockManager.getManager().dumpAllLocks();
			}
			catch ( Throwable t )
			{
				// Don't use logging module, explicitly use stderr
				System.err.println( "Unable to dump Neo command stack and " +
									"lock information: " + t );
			}
			neo.shutdown();
		}
	}
	
}	

