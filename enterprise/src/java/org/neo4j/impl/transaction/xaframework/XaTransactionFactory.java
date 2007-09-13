package org.neo4j.impl.transaction.xaframework;

import java.util.List;
import javax.transaction.xa.XAException;

/**
 * Factory for creating {@link XaTransaction XaTransactions} used during
 * recovery.
 */
public abstract class XaTransactionFactory
{
	private XaLogicalLog log;
	
	/**
	 * Create a {@link XaTransaction} with <CODE>identifier</CODE> as internal 
	 * transaction id.
	 *
	 * @param identifier The identifier of the transaction
	 * @return A new xa transaction
	 */
	public abstract XaTransaction create( int identifier );
	
	/**
	 * If lazy done is activated in {@link XaResourceManager} meaning a done 
	 * record isn't written to the logical at once, instead many done are 
	 * collected and written at once. Before these done records are written
	 * this method will be called with a list containing all the transaction
	 * identifiers for the done records that will be written. The purpose
	 * is ofcourse to make sure all changes made by those transactions are 
	 * flushed before the done record is written.
	 * 
	 * @param identifiers list of transaction identifiers that will have their
	 * done record written to logical log
	 * 
	 * @throws XAException if unable to perform operation
	 */
	public abstract void lazyDoneWrite( List<Integer> identifiers ) 
		throws XAException;
	
	void setLogicalLog( XaLogicalLog log )
	{
		this.log = log;
	}
	
	protected XaLogicalLog getLogicalLog()
	{
		return log;
	}
	
	/**
	 * This method will be called when all recovered transactions have been
	 * resolved to a safe state (rolledback or committed). This implementation 
	 * does nothing so override if you need to do something when recovery 
	 * is complete.
	 */
	public void recoveryComplete() {}
}
