package org.neo4j.impl.transaction.xaframework;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


/**
 * Help implementation of the XAResource interface. See 
 * {@link XaConnectionHelpImpl} for more information.
 */
public abstract class XaResourceHelpImpl implements XAResource
{
	private int transactionTimeout = 120;
	private XaTransaction xaTx = null;
	private XaResourceManager xaRm = null;
	
	protected XaResourceHelpImpl( XaResourceManager xaRm )
	{
		this.xaRm = xaRm;
	}
	
	/** 
	 * If the transaction commited successfully this method will return the 
	 * transaction.
	 *
	 * @return transaction if completed else <CODE>null</CODE>
	 */
	public XaTransaction getCompletedTx()
	{
		return xaTx;
	}
	
	/**
	 * Should return true if <CODE>xares</CODE> is the same resource as 
	 * <CODE>this</CODE>.
	 *
	 * @return true if the resource is same
	 */
	public abstract boolean isSameRM( XAResource xares );
	
	public void commit( Xid xid, boolean onePhase ) throws XAException
	{
		xaTx = xaRm.commit( xid, onePhase );
	}
	
	public void end( Xid xid, int flags ) throws XAException
	{
		if ( flags == XAResource.TMSUCCESS )
		{
			xaRm.end( this, xid );
		}
		else if ( flags == XAResource.TMSUSPEND )
		{
			xaRm.suspend( xid );
		}
		else if ( flags == XAResource.TMFAIL )
		{
			xaRm.fail( this, xid );
		}
	}
	
	public void forget( Xid xid ) throws XAException
	{
		// throw heuristic if state > COMMITTING
		xaRm.forget( xid );
	}
	
	public int getTransactionTimeout()
	{
		return transactionTimeout;
	}
	
	public boolean setTransactionTimeout( int timeout )
	{
		transactionTimeout = timeout;
		return true;
	}
	
	public int prepare( Xid xid ) throws XAException
	{ 
		return xaRm.prepare( xid );
	}
	
	public Xid[] recover( int flag ) throws XAException
	{ 
		return xaRm.recover( flag );
	}
	
	public void rollback( Xid xid ) throws XAException
	{
		xaRm.rollback( xid );
	}
	
	public void start( Xid xid, int flags ) throws XAException
	{
		xaTx = null;
		if ( flags == XAResource.TMNOFLAGS )
		{
			xaRm.start( this, xid );
		}
		else if ( flags == XAResource.TMRESUME )
		{
			xaRm.resume( xid );
		}
		else if ( flags == XAResource.TMJOIN )
		{
			xaRm.join( this, xid );
		}
		else
		{
			throw new XAException( "Unkown flag[" + flags + "]" );
		}
	}
}
