package org.neo4j.api.core;

import org.neo4j.impl.transaction.TransactionUtil;

/**
 * A utility class to manage transactions in Neo. All operations that work with
 * the node space (even read operations) must be wrapped in a transaction.
 * Fortunately, the Transaction class makes this very easy. Here's the idiomatic
 * use of transactions in Neo:<pre><code> Transaction tx = Transaction.begin();
 * try
 * {
 * 	... // any operation that works with the node space
 *	tx.success();
 * }
 * finally
 * {
 * 	tx.finish();
 * } </code></pre>
 * <p>
 * Let's walk through this example line by line. First we retrieve a Transaction
 * object by invoking the static {@link #begin()} factory method. This creates
 * a new Transaction instance which has internal state to keep track of whether
 * the current transaction is successful. Then we wrap all operations that work
 * with the node space in a try-finally block. At the end of the block, we
 * invoke the {@link #finish() tx.success()} method to indicate that the
 * transaction is successful. As we exit the block, the finally clause will
 * kick in and {@link #finish() tx.finish} will commit the transaction if the
 * internal state indicates success or else mark it for rollback.
 * <p>
 * If an exception is raised in the try-block, <code>tx.success()</code> will
 * never be invoked and the internal state of the transaction object will cause
 * <code>tx.finish()</code> to roll back the transaction. This is very
 * important: unless {@link #success()} is invoked, the transaction will fail
 * upon {@link #finish()}. A transaction can be explicitly marked for rollback
 * by invoking the {@link #failure() tx.failure()} method.
 */
public class Transaction
{
	private static final Transaction PLACEBO_TRANSACTION = new Transaction()
	{
		@Override
		public void failure()
		{
			// If a "nested" transaction fails, invoke parent's failure()
			// method, which causes the current transaction to be marked for
			// rollback
			super.failure();
		}

		@Override
		public void finish()
		{
			// Do nothing
		}

		@Override
		public void success()
		{
			// Do nothing
		}
	};
	
	private boolean success = false;
	
	/**
	 * Private constructor.
	 */
	private Transaction()
	{
	}
	
	/**
	 * Starts a new transaction.
	 * @return a transaction object representing the current transaction
	 */
	public static Transaction begin()
	{
		if ( TransactionUtil.beginTx() )
		{
			return new Transaction();			
		}
		else
		{
			return PLACEBO_TRANSACTION;
		}		
	}
	
	/**
	 * Marks this transaction as failed, which means that it will inexplicably be
	 * rolled back upon invocation of {@link #finish()}. Once this method has
	 * been invoked, it doesn't matter how many times {@link #success()} is
	 * invoked -- the transaction will still be rolled back.
	 */
	public void failure()
	{
		this.success = false;
		TransactionUtil.markAsRollbackOnly();
	}
	
	/**
	 * Marks this transaction as successful, which means that it will be
	 * commited upon invocation of {@link #finish()} unless {@link #failure()}
	 * has or will be invoked before then.
	 */
	public void success()
	{
		success = true;
	}
	
	/**
	 * Commits or marks this transaction for rollback, depending on whether
	 * {@link #success()} or {@link #failure()} has been previously invoked.
	 */
	public void finish()
	{
		TransactionUtil.finishTx( success, true );
	}
}
