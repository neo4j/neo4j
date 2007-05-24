package org.neo4j.impl.transaction.xaframework;

import javax.transaction.xa.XAResource;


/**
 * <CODE>XaConnection</CODE> holds the <CODE>XAResource</CODE> used by the 
 * transaction manager to control the work done on the resource in a global
 * transaction. Normally you add your "work like" methods in the implementing 
 * class.
 * <p>
 * Don't forget to enlist and delist the resource with the transaction manager
 * when work is beeing done 
 *
 * @see XaConnectionHelpImpl
 */
public interface XaConnection
{
	/**
	 * Returns the <CODE>XAResource</CODE> held by this 
	 * <CODE>XaConnection</CODE>.
	 *
	 * @return The resource associated with this connection
	 */
	public XAResource getXaResource(); 
	
	/**
	 * Destroys this connection and depending on <CODE>XAResource</CODE>
	 * implementation the work done on the resource will be rolled back, 
	 * saved or committed (may also depend on transactional state).
	 * <p>
	 * This method is only valid to call once and there after it is illegal to 
	 * invoke <CODE>getXaResource</CODE> or any other method that creates 
	 * transactions or performs work using the <CODE>XAResource</CODE>.
	 */
	public void destroy();
}
