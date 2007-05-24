package org.neo4j.impl.transaction.xaframework;

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
