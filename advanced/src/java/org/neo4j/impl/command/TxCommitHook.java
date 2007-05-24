package org.neo4j.impl.command;

import java.util.logging.Logger;

import javax.transaction.Synchronization;
import javax.transaction.Status;

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
		boolean success = false;
		try
		{
			switch ( param )
			{
				case Status.STATUS_COMMITTED:
					CommandManager.getManager().releaseCommands();
					success = true;
					break;
				case Status.STATUS_ROLLEDBACK:
					CommandManager.getManager().undoAndReleaseCommands();
					success = true;
					break;
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
		}
		catch ( javax.transaction.InvalidTransactionException ite )
		{
			ite.printStackTrace();
			log.severe( "Unable to release/undo commands." );
		}
		catch ( UndoFailedException e )
		{
			e.printStackTrace();
			log.severe( "Error undoing commands" );
			throw e;
		}
		finally
		{
			TransactionCache.getCache().cleanCurrentTransaction();
		}
		if ( !success )
		{
			try
			{
				log.severe( "Forcing release of commands" );
				CommandManager.getManager().undoAndReleaseCommands();
			}
			catch ( javax.transaction.InvalidTransactionException e )
			{
				e.printStackTrace();
				log.severe( "Force release failed" );
			}
			finally
			{
				TransactionCache.getCache().cleanCurrentTransaction();
			}
		}
	}
}

