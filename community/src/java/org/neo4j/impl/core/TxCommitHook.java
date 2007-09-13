package org.neo4j.impl.core;

import java.util.logging.Logger;

import javax.transaction.Synchronization;

class TxCommitHook implements Synchronization
{
	private static Logger log = Logger.getLogger( 
		TxCommitHook.class.getName() );
	
	public void beforeCompletion()
	{
	}

	public void afterCompletion( int param )
	{
		try
		{
			this.releaseLocks( param );
		}
		catch ( Throwable t )
		{
			t.printStackTrace();
			log.severe( "Unable to release commands" );
		}
	}
	
	private void releaseLocks( int param )
	{
		LockReleaser.getManager().releaseLocks();
	}
}

