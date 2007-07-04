package org.neo4j.impl.nioneo.xa;

import org.neo4j.impl.transaction.xaframework.XaLogicalLog;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds information about the current transaction such as recovery mode
 * and transaction identifier.
 */
public class TxInfoManager
{
	private static TxInfoManager txManager = new TxInfoManager();
	
	private Map<Thread,Boolean> txMode = new HashMap<Thread,Boolean>();
	
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
		txMode.put( Thread.currentThread(), Boolean.valueOf( mode ) );
	}
	
	void unregisterMode()
	{
		txMode.remove( Thread.currentThread() );
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
		Boolean b = txMode.get( Thread.currentThread() );
		if ( b == null )
		{
			return false;
		}
		return b;
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
