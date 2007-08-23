package org.neo4j.impl.transaction;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

/**
 * Use this factory to get hold of a {@link UserTransaction} or to set the
 * {@link TransactionIsolationLevel}.
 */
public class TransactionFactory
{
	private static final TxManager txManager = TxManager.getManager();
	
	public static TransactionManager getTransactionManager()
	{
		return txManager;
	}
	
	/**
	 * Returns a {@link UserTransaction} instance.
	 * 
	 * @return A user transaction 
	 */
	public static UserTransaction getUserTransaction()
	{
		return UserTransactionImpl.getInstance();
	}
	
	/**
	 * Sets the transaction isolation level to <CODE>level</CODE>. A 
	 * transaction with no commands executed yet must exist. If no transaction 
	 * exist or if commands already have been created in the current 
	 * transaction a {@link InvalidTransactionException} is thrown.
	 *
	 * @param level The transaction isolationlevel to set for current 
	 * transaction
	 * @throws InvalidTransactionException
	 */
//	public static void setTransactionIsolationLevel( 
//		TransactionIsolationLevel level ) throws InvalidTransactionException
//	{
//		try
//		{
//			LockReleaser.getManager().setTransactionIsolationLevel( level );
//		}
//		catch ( NotInTransactionException e )
//		{
//			throw new InvalidTransactionException( "" + e );
//		}
//	}
	
	/**
	 * Returns the current {@link TransactionIsolationLevel}. If no transaction
	 * is present a {@link NotInTransactionException} is thrown.
	 * 
	 * @return The transaction isolation level
	 * @throws NotInTransactionException
	 */
//	public static TransactionIsolationLevel getTransactionIsolationLevel()
//		throws NotInTransactionException
//	{
//		return LockReleaser.getManager().getTransactionIsolationLevel();
//	}
}