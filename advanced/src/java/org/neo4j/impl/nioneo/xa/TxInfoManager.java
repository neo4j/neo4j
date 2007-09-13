package org.neo4j.impl.nioneo.xa;

import org.neo4j.impl.transaction.xaframework.XaLogicalLog;

/**
 * Holds information about the current transaction such as recovery mode
 * and transaction identifier.
 */
public class TxInfoManager
{
	private static final TxInfoManager txManager = new TxInfoManager();
	
	private boolean recoveryMode = false;
	
	public static TxInfoManager getManager()
	{
		return txManager;
	}
	
	private XaLogicalLog log = null;
	
	public void setRealLog( XaLogicalLog log )
	{
		this.log = log;
	}
	
	void registerMode( boolean mode )
	{
		this.recoveryMode = mode;
	}
	
	void unregisterMode()
	{
		recoveryMode = false;
	}
	
	/**
	 * Returns <CODE>true</CODE> if the current transaction is in 
	 * recovery mode. If current thread doesn't have a transaction 
	 * <CODE>false</CODE> is returned.
	 * 
	 * @return True if current transaction is in recovery mode
	 */
	public boolean isInRecoveryMode()
	{
		return recoveryMode;
	}
	
	/**
	 * Returns the current transaction identifier. If current thread doesn't
	 * have a transaction <CODE>-1</CODE> is returned.
	 * 
	 * @return The current transaction identifier
	 */
	public int getCurrentTxIdentifier()
	{
		if ( log == null )
		{
			return -1;
		}
		return log.getCurrentTxIdentifier();
	}
}
