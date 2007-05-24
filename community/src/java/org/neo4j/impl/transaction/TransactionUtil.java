package org.neo4j.impl.transaction;

import javax.transaction.Status;
import javax.transaction.UserTransaction;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;

/**
 * Utility class to help make use of transaction. Example of usage:
 * <pre>
 * <CODE>
 * boolean txStarted = TransactionUtil.beginTx();
 * boolean success = false;
 * try
 * {
 * 	// do some transactional work
 * 	success = true;
 * }
 * catch ( SomeException e )
 * { 
 * 	// ok success is false
 * }
 * finally
 * {
 *	TransactionUtil.finishTx( success, txStarted );
 * }
 * </CODE>
 * </pre>
 * The code example above makes sure no nested transactions are opened 
 * (since Neo doesn't support that) and either commits or rollbacks the 
 * transaction depending on the success flag if the transaction was started
 * in this try - finally block (txStarted flag).
 */
public class TransactionUtil
{
	/**
	 * Tries to start a transaction for the current thread if no transaction
	 * is currently active.
	 *
	 * @returns <code>true</code> if a this method starts the transaction.
	 * returns <code>false</code> if a transaction is already started.
	 * @throws SomeException if an error occurs.
	 */
	public static boolean beginTx()
	{
		try
		{
			UserTransaction ut = TransactionFactory.getUserTransaction();
			if ( ut.getStatus() == Status.STATUS_NO_TRANSACTION )
			{
				try
				{
					ut.begin();
					EventManager.getManager().generateProActiveEvent(
						Event.TX_IMMEDIATE_BEGIN, new EventData(
							getTransactionId( ut ) ) );
					return true;
				}
				catch ( javax.transaction.NotSupportedException e )
				{
					throw new RuntimeException(
						"Unable to begin transaction", e );
				}
				catch ( javax.transaction.SystemException e )
				{
					throw new RuntimeException(
						"Unable to begin transaction", e );
				}
			}
			return false;
		}
		catch ( javax.transaction.SystemException e )
		{
			throw new RuntimeException(
				"Unable to verify transaction context", e );
		}
	}
	
	/**
	 * Tries to commit a transaction if <code>txStarted</code>
	 * is <code>true</code>. <code>txStarted</code> should be the value returned
	 * by beginTx() i.e. if beginTx() started a transaction that needs to be
	 * commited.
	 *
	 * @param txStarted decides whether to actually commit or not.
	 */
	public static void commitTx( boolean txStarted )
	{
		if ( !txStarted )
		{
			return;
		}
		
		UserTransaction ut = null;
		boolean committed = false;
		Object txId = null;
		try
		{
			ut = TransactionFactory.getUserTransaction();
			txId = getTransactionId( ut );
			ut.commit();
			committed = true;
		}
		catch ( Exception e )
		{
			try
			{
				if ( ut != null && ut.getStatus() != 
					Status.STATUS_NO_TRANSACTION )
				{
					ut.rollback();
				}
			}
			catch ( Exception ee )
			{
				throw new RuntimeException(
					"Unable to rollback commit operation " + ee, ee );
			}
			throw new RuntimeException( "Unable to commit operation", e );
		}
		finally
		{
			txEnded( committed, txId );
		}
	}
	
	/**
	 * Tries to rollback a transaction if <code>txStarted</code>
	 * is <code>true</code>. <code>txStarted</code> should be the value returned
	 * by beginTx() i.e. if beginTx() started a transaction that needs to be
	 * rolled back.
	 *
	 * @param txStarted decides whether to actually rollback or not.
	 */
	public static void rollbackTx( boolean txStarted )
	{
		if ( !txStarted )
		{
			return;
		}
		
		Object txId = null;
		try
		{
			UserTransaction ut = TransactionFactory.getUserTransaction();
			txId = getTransactionId( ut );
			if ( ut.getStatus() != Status.STATUS_NO_TRANSACTION )
			{
				ut.rollback();
			}
		}
		catch ( Exception e )
		{
			throw new RuntimeException(
				"Unable to rollback operation", e );
		}
		finally
		{
			txEnded( false, txId );
		}
	}
	
	/**
	 * Usually use this in a <code>finally</code> block and nowhere else in
	 * the method. Useful in order to not forget to commit/rollback
	 * the transaction. F.ex.
	 * <pre>
	 * <CODE>
	 *	void method()
	 *	{
	 *	 	boolean txStarted = TransactionUtil.beginTx();
	 *		boolean success = false;
	 *		try
	 *		{
	 *	 		// perform some code here.
	 *			
	 *			success = true;
	 *			return someThing; // without worry
	 *		}
	 *		catch ( ...Exception e )
	 *		{
	 *			success = false;
	 *			throw SomeException( e );
	 *		}
	 *		finally
	 *		{
	 *			TransactionUtil.finishTx( success, txStarted );
	 *		}
	 *	}
	 * </CODE>
	 * </pre>
	 * @param transactionSucceeded Flag telling if transaction should 
	 * be committed or rolledback
	 * @param txStarted Flag that tells if the transaction was started in the 
	 * current try - finally block
	 */
	public static void finishTx( boolean transactionSucceeded,
		boolean txStarted )
	{
		if ( !txStarted )
		{
			return;
		}
		
		if ( transactionSucceeded )
		{
			commitTx( txStarted );
		}
		else
		{
			rollbackTx( txStarted );
		}
	}
	
	/**
	 * Marks the current transaction as rollback only. Returns 
	 * <CODE>true</CODE> if the transaction was set to rollback only, false 
	 * if we operation failed or there is no transaction
	 *
	 * @return <CODE>true</CODE> if successful
	 */
	public static boolean markAsRollbackOnly()
	{
		try
		{
			TransactionFactory.getUserTransaction().setRollbackOnly();
			return true;
		}
		catch ( Exception e )
		{ 
			return false;
		} // ok we failed
	}
	
	/**
	 * Returns <CODE>false</CODE> if transaction status is 
	 * <CODE>STATUS_NO_TRANSACTION</CODE>, else <CODE>true</CODE> is returned.
	 */
	public static boolean transactionStatus()
	{
		try
		{
			UserTransaction ut = TransactionFactory.getUserTransaction();
			if ( ut.getStatus() == Status.STATUS_NO_TRANSACTION )
			{
				return false;
			}
			return true;
		}
		catch ( javax.transaction.SystemException e )
		{
			throw new RuntimeException(
				"Unable to verify transaction context", e );
		}
	}
	
	private static Object getTransactionId( UserTransaction ut )
	{
		return ( ( UserTransactionImpl ) ut ).getEventIdentifier();
	}

	private static void txEnded( boolean committed, Object txId )
	{
		Event event = committed ? Event.TX_IMMEDIATE_COMMIT :
			Event.TX_IMMEDIATE_ROLLBACK;
		EventManager.getManager().generateProActiveEvent(
			event, new EventData( txId ) );
	}

	private TransactionUtil()
	{
	}
}
