package org.neo4j.impl.transaction.xaframework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.neo4j.impl.util.ArrayMap;

// make package access?
public class XaResourceManager
{
	private final ArrayMap<XAResource,Xid> xaResourceMap = 
		new ArrayMap<XAResource,Xid>();
	private final ArrayMap<Xid,XidStatus> xidMap = 
		new ArrayMap<Xid,XidStatus>();
	private int recoveredTxCount = 0;
	
	private XaLogicalLog log = null;
	private final XaTransactionFactory tf;
	private boolean lazyDone = false;
	private List<Integer> lazyDoneRecords = null;
	
	XaResourceManager( XaTransactionFactory tf )
	{
		this.tf = tf;
	}
	
	/**
	 * If set to <CODE>true</CODE> done records will not be written at once.
	 * Instead a few done records will be collected then written together after 
	 * the {@link XaTransactionFactory.lazyDoneWrite} method has been called.
	 *  
	 * @param status <CODE>true</CODE> turns lazy write of done records on, 
	 * <CODE>false</CODE> turns it off
	 * 
	 * @throws XAException if change of lazy done failed
	 */
	public synchronized void setLazyDoneRecords( boolean status ) 
		throws XAException 
	{
		if ( status )
		{
			lazyDone = true;
			lazyDoneRecords = new ArrayList<Integer>();
		}
		else
		{
			lazyDone = false;
			if ( lazyDoneRecords != null )
			{
				tf.lazyDoneWrite( lazyDoneRecords );
				for ( int identifier : lazyDoneRecords )
				{
					log.done( identifier );
				}
				lazyDoneRecords = null;
			}
		}
	}
	
	synchronized void writeOutLazyDoneRecords() throws XAException
	{
		if ( lazyDone )
		{
			tf.lazyDoneWrite( lazyDoneRecords );
			for ( int identifier : lazyDoneRecords )
			{
				log.done( identifier );
			}
			lazyDoneRecords = null;
		}
		lazyDone = false;
	}
	
	/**
	 * @return true if lazy done is set
	 */
	public boolean lazyDoneSet()
	{
		return lazyDone;
	}
	
	void setLogicalLog( XaLogicalLog log )
	{
		this.log = log;
	}
	
	synchronized XaTransaction getXaTransaction( XAResource xaRes )
		throws XAException
	{
		XidStatus status = xidMap.get( xaResourceMap.get( xaRes ) );
		if ( status == null )
		{
			throw new XAException( "Resource[" + xaRes + "] not enlisted" );
		}
		return status.getTransactionStatus().getTransaction();
	}
	
	synchronized void start( XAResource xaResource, Xid xid ) 
		throws XAException
	{
		if ( xaResourceMap.get( xaResource ) != null )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] already enlisted or suspended" );
		}
		xaResourceMap.put( xaResource, xid );
		if ( xidMap.get( xid ) == null )
		{
			int identifier = log.start( xid );
			XaTransaction xaTx = tf.create( identifier );
			xidMap.put( xid, new XidStatus( xaTx ) );
		}
	}
	
	synchronized void injectStart( Xid xid, XaTransaction tx )
		throws IOException
	{
		if ( xidMap.get( xid ) != null )
		{
			throw new IOException( "Inject start failed, xid: " + xid + 
				" already injected" );
		}
		xidMap.put( xid, new XidStatus( tx ) );
		recoveredTxCount++;
	}
	
	synchronized void resume( Xid xid ) throws XAException
	{
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
	
		if ( status.getActive() )
		{
			throw new XAException( "Xid [" + xid + "] not suspended" );
		}
		status.setActive( true );
	}


	synchronized void join( XAResource xaResource, Xid xid ) throws XAException
	{
		if ( xidMap.get( xid ) == null )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		if ( xaResourceMap.get( xaResource ) != null )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] already enlisted" );
		}
		xaResourceMap.put( xaResource, xid );
	}

	synchronized void end( XAResource xaResource, Xid xid ) throws XAException
	{
		Xid xidEntry = xaResourceMap.remove( xaResource );
		if ( xidEntry == null )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] not enlisted" );
		}
	}

	synchronized void suspend( Xid xid ) throws XAException
	{
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		if ( !status.getActive() )
		{
			throw new XAException( "Xid[" + xid + "] already suspended" );
		}
		status.setActive( false );
	}
	
	synchronized void fail( XAResource xaResource, Xid xid ) throws XAException
	{
		if ( xidMap.get( xid ) == null )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
		Xid xidEntry = xaResourceMap.remove( xaResource );
		if ( xidEntry == null )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] not enlisted" );
		}
		XidStatus status = xidMap.get( xid );
		status.getTransactionStatus().markAsRollback();
	}

	synchronized void validate( XAResource xaResource ) throws XAException
	{
		XidStatus status = xidMap.get( xaResourceMap.get( xaResource ) );
		if ( status == null )
		{
			throw new XAException( "Resource[" + xaResource + 
				"] not enlisted" );
		}
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
		private boolean prepared = false;
		private boolean commitStarted = false;
		private boolean rollback = false;
		private final XaTransaction xaTransaction;
		
		TransactionStatus( XaTransaction xaTransaction )
		{
			this.xaTransaction = xaTransaction;
		}

		void markAsPrepared()
		{
			prepared = true;
		}
		
		void markAsRollback()
		{
			rollback = true;
		}
		
		void markCommitStarted()
		{
			commitStarted = true;
		}
		
		boolean prepared()
		{
			return prepared;
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
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
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
			txStatus.markAsPrepared();
			return XAResource.XA_OK;
		}
	}
	
	// called from XaResource internal recovery
	// returns true if read only and should be removed...
	synchronized boolean injectPrepare( Xid xid ) throws IOException
	{
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new IOException( "Unkown xid[" + xid + "]" );
		}
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
			txOrderMap.put( xid, nextTxOrder++ );
			txStatus.markAsPrepared();
			return false;
		}
	}

	private Map<Xid,Integer> txOrderMap = new HashMap<Xid,Integer>();
	private int nextTxOrder = 0;
	
	// called during recovery
	// if not read only transaction will be commited.
	synchronized void injectOnePhaseCommit( Xid xid ) throws IOException
	{
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new IOException( "Unkown xid[" + xid + "]" );
		}
		TransactionStatus txStatus = status.getTransactionStatus();
		txOrderMap.put( xid, nextTxOrder++ );
		txStatus.markAsPrepared();
		txStatus.markCommitStarted();
	}
	
	synchronized XaTransaction commit( Xid xid, boolean onePhase ) 
		throws XAException 
	{
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
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
			txStatus.markAsPrepared();
		}
		if ( !txStatus.prepared() || txStatus.rollback() )
		{
			throw new XAException( "Transaction not prepared or " + 
				"(marked as) rolledbacked" );
		}
		if ( !xaTransaction.isReadOnly() )
		{
			txStatus.markCommitStarted();
			xaTransaction.commit();
		}
		if ( lazyDone && !xaTransaction.isRecovered() )
		{
			lazyDoneRecords.add( xaTransaction.getIdentifier() );
			if ( lazyDoneRecords.size() >= 100 )
			{
				tf.lazyDoneWrite( lazyDoneRecords );
				for ( int identifier : lazyDoneRecords )
				{
					log.done( identifier );
				}
				lazyDoneRecords = new ArrayList<Integer>();
			}
		}
		else
		{
			log.done( xaTransaction.getIdentifier() );
		}
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
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
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
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new XAException( "Unkown xid[" + xid + "]" );
		}
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
		XidStatus status = xidMap.get( xid );
		if ( status == null )
		{
			throw new IOException( "Unkown xid[" + xid + "]" );
		}
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
		Collections.sort( xids, new Comparator<Xid>()
			{
				public int compare( Xid o1, Xid o2 )
                {
					
					Integer id1 = txOrderMap.get( o1 );
					Integer id2 = txOrderMap.get( o2 );
					if ( id1 == null && id2 == null )
					{
						return 0;
					}
					if ( id1 == null )
					{
						return Integer.MAX_VALUE;
					}
					if ( id2 == null )
					{
						return Integer.MIN_VALUE;
					}
	                return id1 - id2;
                }
			} );
		txOrderMap = null;
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
					System.out.println( "(Re-)committing tx " + identifier );
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
				// else if ( !txStatus.commit() )
				else if ( !txStatus.prepared() )
				{
					System.out.println( "Rolling back non prepared tx " + 
						identifier );
					log.doneInternal( xaTransaction.getIdentifier() );
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
			log.makeNewLog();
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