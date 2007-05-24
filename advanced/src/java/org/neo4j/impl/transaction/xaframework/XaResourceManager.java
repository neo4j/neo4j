package org.neo4j.impl.transaction.xaframework;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// make package access?
public class XaResourceManager
{
	private Map<XAResource,Xid> xaResourceMap = 
		new HashMap<XAResource,Xid>();
	private Map<Xid,XidStatus> xidMap = new HashMap<Xid,XidStatus>();
	private int recoveredTxCount = 0;
	
	private XaLogicalLog log = null;
	private XaTransactionFactory tf = null;
	
	XaResourceManager( XaTransactionFactory tf )
	{
		this.tf = tf;
	}
	
	void setLogicalLog( XaLogicalLog log )
	{
		this.log = log;
	}
	
	synchronized XaTransaction getXaTransaction( XAResource xaRes )
		throws XAException
	{
		if ( !xaResourceMap.containsKey( xaRes ) )
		{
			throw new XAException( "Resource[" + xaRes + "] not enlisted" );
		}
		XidStatus status = xidMap.get( xaResourceMap.get( xaRes ) );
		return status.getTransactionStatus().getTransaction();
	}
	
	synchronized void start( XAResource xaResource, Xid xid ) 
		throws XAException
	{
		if ( xaResourceMap.containsKey( xaResource ) )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] already enlisted or suspended" );
		}
		xaResourceMap.put( xaResource, xid );
		if ( !xidMap.containsKey( xid ) )
		{
			int identifier = log.start( xid );
			XaTransaction xaTx = tf.create( identifier );
			xidMap.put( xid, new XidStatus( xaTx ) );
		}
	}
	
	synchronized void injectStart( Xid xid, XaTransaction tx )
		throws IOException
	{
		if ( xidMap.containsKey( xid ) )
		{
			throw new IOException( "Inject start failed, xid: " + xid + 
				" already injected" );
		}
		xidMap.put( xid, new XidStatus( tx ) );
		recoveredTxCount++;
	}
	
	synchronized void resume( Xid xid ) throws XAException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
	
		XidStatus status = xidMap.get( xid );
		if ( status.getActive() )
		{
			throw new XAException( "Xid [" + xid + "] not suspended" );
		}
		status.setActive( true );
	}


	synchronized void join( XAResource xaResource, Xid xid ) throws XAException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		if ( xaResourceMap.containsKey( xaResource ) )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] already enlisted" );
		}
		xaResourceMap.put( xaResource, xid );
	}

	synchronized void end( XAResource xaResource, Xid xid ) throws XAException
	{
		if ( !xaResourceMap.containsKey( xaResource ) )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] not enlisted" );
		}
		xaResourceMap.remove( xaResource );
	}

	synchronized void suspend( Xid xid ) throws XAException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		XidStatus status = xidMap.get( xid );
		if ( !status.getActive() )
		{
			throw new XAException( "Xid[" + xid + "] already suspended" );
		}
		status.setActive( false );
	}
	
	synchronized void fail( XAResource xaResource, Xid xid ) throws XAException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		if ( !xaResourceMap.containsKey( xaResource ) )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] not enlisted" );
		}
		xaResourceMap.remove( xaResource );
		XidStatus status = xidMap.get( xid );
		status.getTransactionStatus().markAsRollback();
	}

	synchronized void validate( XAResource xaResource ) throws XAException
	{
		if ( !xaResourceMap.containsKey( xaResource ) )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] not enlisted" );
		}
		XidStatus status = xidMap.get( xaResourceMap.get( xaResource ) );
		if ( !status.getActive() )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] suspended" );
		}
	}
	
	// TODO: check so we're not currently committing on the resource
	synchronized void destroy( XAResource xaResource )
	{
		xaResourceMap.remove( xaResource );
	}
	
	private static class XidStatus
	{
		private boolean active = true;
		private TransactionStatus txStatus;
		
		XidStatus( XaTransaction xaTransaction )
		{
			txStatus = new TransactionStatus( xaTransaction );
		}

		void setActive( boolean active )
		{
			this.active = active;
		}
		
		boolean getActive()
		{
			return this.active;
		}
		
		TransactionStatus getTransactionStatus()
		{
			return txStatus;
		}
	}

	private static class TransactionStatus
	{
		private boolean commit = false;
		private boolean commitStarted = false;
		private boolean rollback = false;
		private XaTransaction xaTransaction = null;
		
		TransactionStatus( XaTransaction xaTransaction )
		{
			this.xaTransaction = xaTransaction;
		}

		void markAsCommit()
		{
			rollback = false;
			commit = true;
		}
		
		void markAsRollback()
		{
			commit = false;
			rollback = true;
		}
		
		void markCommitStarted()
		{
			commitStarted = true;
		}
		
		boolean commit()
		{
			return commit;
		}
		
		boolean rollback()
		{
			return rollback;
		}
		
		boolean commitStarted()
		{
			return commitStarted;
		}
		
		XaTransaction getTransaction()
		{
			return xaTransaction;
		}
	}
	
	synchronized int prepare( Xid xid ) throws XAException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		XidStatus status = xidMap.get( xid );
		TransactionStatus txStatus = status.getTransactionStatus();
		XaTransaction xaTransaction = txStatus.getTransaction();
		if ( xaTransaction.isReadOnly() )
		{
			log.done( xaTransaction.getIdentifier() );
			xidMap.remove( xid );
			if ( xaTransaction.isRecovered() )
			{
				recoveredTxCount--;
				checkIfRecoveryComplete();
			}
			return XAResource.XA_RDONLY;
		}
		else
		{
			xaTransaction.prepare();
			log.prepare( xaTransaction.getIdentifier() );
			txStatus.markAsCommit();
			return XAResource.XA_OK;
		}
	}
	
	// called from XaResource internal recovery
	// returns true if read only and should be removed...
	synchronized boolean injectPrepare( Xid xid ) throws IOException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new IOException( "Unkown xid[" + xid + "]" );
		}
		XidStatus status = xidMap.get( xid );
		TransactionStatus txStatus = status.getTransactionStatus();
		XaTransaction xaTransaction = txStatus.getTransaction();
		if ( xaTransaction.isReadOnly() )
		{
			xidMap.remove( xid );
			if ( xaTransaction.isRecovered() )
			{
				recoveredTxCount--;
				checkIfRecoveryComplete();
			}
			return true;
		}
		else
		{
			txStatus.markAsCommit();
			return false;
		}
	}

	// called during recovery
	// if not read only transaction will be commited.
	synchronized void injectOnePhaseCommit( Xid xid ) throws IOException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new IOException( "Unkown xid[" + xid + "]" );
		}
		XidStatus status = ( XidStatus ) xidMap.get( xid );
		TransactionStatus txStatus = status.getTransactionStatus();
//		XaTransaction xaTransaction = txStatus.getTransaction();
		txStatus.markCommitStarted();
	}
	
	synchronized XaTransaction commit( Xid xid, boolean onePhase ) 
		throws XAException 
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		XidStatus status = xidMap.get( xid );
		TransactionStatus txStatus = status.getTransactionStatus();
		XaTransaction xaTransaction = txStatus.getTransaction();
		if ( onePhase )
		{
			if ( !xaTransaction.isReadOnly() )
			{
				if ( !xaTransaction.isRecovered() )
				{
					xaTransaction.prepare();
				}
				log.commitOnePhase( xaTransaction.getIdentifier() );
			}
			txStatus.markAsCommit();
		}
		if ( !txStatus.commit() && txStatus.rollback() )
		{
			throw new XAException( "Transaction not prepared or " + 
				"(marked as) rolledbacked" );
		}
		if ( !xaTransaction.isReadOnly() )
		{
			txStatus.markCommitStarted();
			xaTransaction.commit();
		}
		log.done( xaTransaction.getIdentifier() );
		xidMap.remove( xid );
		if ( xaTransaction.isRecovered() )
		{
			recoveredTxCount--;
			checkIfRecoveryComplete();
		}
		return xaTransaction;
	}
	
	synchronized XaTransaction rollback( Xid xid ) throws XAException 
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		XidStatus status = xidMap.get( xid );
		TransactionStatus txStatus = status.getTransactionStatus();
		XaTransaction xaTransaction = txStatus.getTransaction();
		if ( txStatus.commitStarted() )
		{
			throw new XAException( "Transaction already started commit" );
		}
		txStatus.markAsRollback();
		xaTransaction.rollback();
		log.done( xaTransaction.getIdentifier() );
		xidMap.remove( xid );
		if ( xaTransaction.isRecovered() )
		{
			recoveredTxCount--;
			checkIfRecoveryComplete();
		}
		return txStatus.getTransaction();
	}
	
	synchronized XaTransaction forget( Xid xid ) throws XAException
	{
		XidStatus status = xidMap.get( xid );
		TransactionStatus txStatus = status.getTransactionStatus();
		XaTransaction xaTransaction = txStatus.getTransaction();
		log.done( xaTransaction.getIdentifier() );
		xidMap.remove( xid );
		if ( xaTransaction.isRecovered() )
		{
			recoveredTxCount--;
			checkIfRecoveryComplete();
		}
		return xaTransaction;
	}
	
	synchronized void markAsRollbackOnly( Xid xid ) throws XAException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		XidStatus status = xidMap.get( xid );
		TransactionStatus txStatus = status.getTransactionStatus(); 
		txStatus.markAsRollback();
	}
	
	synchronized Xid[] recover( int flag ) throws XAException
	{
		List<Xid> xids = new ArrayList<Xid>();
		Iterator<Xid> keyIterator = xidMap.keySet().iterator();
		while ( keyIterator.hasNext() )
		{
			xids.add( keyIterator.next() );
		}
		return xids.toArray( new Xid[ xids.size() ] );
	}
	
	// called from neostore internal recovery
	synchronized void pruneXid( Xid xid ) throws IOException
	{
		if ( !xidMap.containsKey( xid ) )
		{
			throw new IOException( "Unkown xid[" + xid + "]" );
		}
		XidStatus status = xidMap.get( xid );
		TransactionStatus txStatus = status.getTransactionStatus();
		XaTransaction xaTransaction = txStatus.getTransaction();
		xidMap.remove( xid );
		if ( xaTransaction.isRecovered() )
		{
			recoveredTxCount--;
			checkIfRecoveryComplete();
		}
	}
	
	synchronized void checkXids() throws IOException
	{
		Iterator<Xid> keyIterator = xidMap.keySet().iterator();
		LinkedList<Xid> xids = new LinkedList<Xid>();
		while ( keyIterator.hasNext() )
		{
			xids.add( keyIterator.next() );
		}
		while ( !xids.isEmpty() )
		{
			Xid xid = xids.removeFirst();
			XidStatus status = xidMap.get( xid );
			TransactionStatus txStatus = status.getTransactionStatus();
			XaTransaction xaTransaction = txStatus.getTransaction();
			int identifier = xaTransaction.getIdentifier();
			if ( xaTransaction.isRecovered() )
			{
				if ( txStatus.commitStarted() )
				{
					System.out.println( "Committing 1PC tx " + identifier );
					try
					{
						xaTransaction.commit();
					}
					catch ( XAException e )
					{
						e.printStackTrace();
						throw new IOException( 
							"Unable to commit one-phase transaction " + 
							identifier + ", " + e );
					}
					log.doneInternal( identifier );
					xidMap.remove( xid );
					recoveredTxCount--;
				}
				else if ( !txStatus.commit() )
				{
					log.doneInternal( xaTransaction.getIdentifier() );
//					System.out.println( "Rollback non prepared tx " + 
//						identifier );
					xidMap.remove( xid );
					recoveredTxCount--;
				}
			}
		}
		checkIfRecoveryComplete();
	}
	
	private void checkIfRecoveryComplete()
	{
		if ( log.scanIsComplete() && recoveredTxCount == 0 )
		{
			tf.recoveryComplete();
		}
	}
	
	// for testing, do not use!
	synchronized void reset()
	{
		xaResourceMap.clear();
		xidMap.clear();
		log.reset();
	}
	
	/**
	 * Returns <CODE>true</CODE> if recovered transactions exist. This method
	 * is useful to invoke after the logical log has been opened to detirmine
	 * if there are any recovered transactions waiting for the TM to tell them
	 * what to do.
	 * 
	 * @return True if recovered transactions exist
	 */
	public boolean hasRecoveredTransactions()
	{
		return recoveredTxCount > 0;
	}
}
