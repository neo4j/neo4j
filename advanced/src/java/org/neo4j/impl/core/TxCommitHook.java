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

	// Tell command pool to release commands/locks/transaction cache
	public void afterCompletion( int param )
	{
		try
		{
			this.releaseCommands( param );
		}
		catch ( Throwable t )
		{
			t.printStackTrace();
			log.severe( "Unable to release commands" );
		}
	}
	
	// If param = commit, release commands
	// If param = rollback, undo and release commands
	private void releaseCommands( int param )
	{
/*		if ( param == Status.STATUS_COMMITTED || 
			param == Status.STATUS_ROLLEDBACK )
		{*/
			LockReleaser.getManager().releaseLocks();
/*		}
		else
		{
			switch ( param )
			{
				case Status.STATUS_ACTIVE:
					log.severe( "Unexpected tx status after completion: " +
							   "STATUS_ACTIVE" );
					break;
				case Status.STATUS_COMMITTING:
					log.severe( "Unexpected tx status after completion: " +
							   "STATUS_COMMITING" );
					break;
				case Status.STATUS_MARKED_ROLLBACK:
					log.severe( "Unexpected tx status after completion: " +
							   "STATUS_MARKED_ROLLBACK" );
					break;
				case Status.STATUS_NO_TRANSACTION:
					log.severe( "Unexpected tx status after completion: " +
							   "STATUS_NO_TRANSACTION" );
					break;
				case Status.STATUS_PREPARED:
					log.severe( "Unexpected tx status after completion: " +
							   "STATUS_PREPARED" );
					break;
				case Status.STATUS_PREPARING:
					log.severe( "Unexpected tx status after completion: " +
							   "STATUS_PREPARING" );
					break;
				case Status.STATUS_ROLLING_BACK:
					log.severe( "Unexpected tx status after completion: " +
							   "STATUS_ROLLING_BACK" );
					break;
				case Status.STATUS_UNKNOWN:
					log.severe( "Unexpected tx status after completion: " +
							   "STATUS_UNKNOWN" );
					break;
				default:
					log.severe( "Unexpected and unknown tx status after " +
							   "completion: [" + param + "]." );
			}
			throw new RuntimeException();
		}*/
	}
}

