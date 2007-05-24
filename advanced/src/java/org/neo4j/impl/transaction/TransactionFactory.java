package org.neo4j.impl.transaction;

import org.neo4j.impl.command.CommandManager;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import javax.transaction.InvalidTransactionException;

/**
 * Use this factory to get hold of a {@link UserTransaction} or to set the
 * {@link TransactionIsolationLevel}.
 */
public class TransactionFactory
{
	/**
	 * Public only for testing purpouse. Use 
	 * {@link #getUserTransaction()} instead.
	 */
	public static TransactionManager getTransactionManager()
	{
		return TxManager.getManager();
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
	public static void setTransactionIsolationLevel( 
		TransactionIsolationLevel level ) throws InvalidTransactionException
	{
		try
		{
			CommandManager.getManager().setTransactionIsolationLevel( level );
		}
		catch ( NotInTransactionException e )
		{
			throw new InvalidTransactionException( "" + e );
		}
	}
	
	/**
	 * Returns the current {@link TransactionIsolationLevel}. If no transaction
	 * is present a {@link NotInTransactionException} is thrown.
	 * 
	 * @return The transaction isolation level
	 * @throws NotInTransactionException
	 */
	public static TransactionIsolationLevel getTransactionIsolationLevel()
		throws NotInTransactionException
	{
		return CommandManager.getManager().getTransactionIsolationLevel();
	}
}