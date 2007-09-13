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
}