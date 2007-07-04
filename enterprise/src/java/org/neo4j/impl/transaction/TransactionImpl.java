package org.neo4j.impl.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

class TransactionImpl implements Transaction
{
	private static Logger log = Logger.getLogger( 
		TransactionImpl.class.getName() );
	
	private static final int RS_ENLISTED = 0;
	private static final int RS_SUSPENDED = 1;
	private static final int RS_DELISTED = 2;
	private static final int RS_READONLY = 3; // set in prepare
	
	private byte globalId[] = null;
	private int status = Status.STATUS_ACTIVE;
	
	private LinkedList<ResourceElement> resourceList = 
		new LinkedList<ResourceElement>();
	private List<Synchronization> syncHooks = 
		new ArrayList<Synchronization>();
	
	private Integer eventIdentifier = null;
	
	private static int eventIdentifierCounter = 0;
	
	private static synchronized int getNextEventIdentifier()
	{
		return eventIdentifierCounter++;
	}
	
	TransactionImpl()
	{
		globalId = XidImpl.getNewGlobalId();
		eventIdentifier = getNextEventIdentifier();
	}
	
	Integer getEventIdentifier()
	{
		return eventIdentifier;
	}
	
	byte[] getGlobalId()
	{
		return globalId;
	}
	
	public synchronized String toString()
	{
		StringBuffer txString = new StringBuffer( "Transaction[Status=" + 
			TxManager.getManager().getTxStatusAsString( status ) + 
			",ResourceList=" );
		Iterator itr = resourceList.iterator();
		while ( itr.hasNext() )
		{
			txString.append( itr.next().toString() );
			if ( itr.hasNext() )
			{
				txString.append( "," );
			}
		}
		return txString.toString();
	}
	
	public synchronized void commit() throws RollbackException, 
		HeuristicMixedException, HeuristicRollbackException, 
		IllegalStateException, SystemException
	{
		// make sure tx not suspended 
		TxManager.getManager().commit();
	}

	public synchronized void rollback() throws IllegalStateException, 
		SystemException 
	{
		// make sure tx not suspended
		TxManager.getManager().rollback();
	}

	public synchronized boolean enlistResource( XAResource xaRes ) 
		throws RollbackException, IllegalStateException, SystemException
	{
		if ( xaRes == null )
		{
			throw new IllegalArgumentException( "Null xa resource" );
		}
		if ( status == Status.STATUS_ACTIVE || 
			status == Status.STATUS_PREPARING )
		{
			try
			{
				if ( resourceList.size() == 0 )
				{
					// 
					byte branchId[] = 
						TxManager.getManager().getBranchId( xaRes ); 
					Xid xid = new XidImpl( globalId, branchId );
					resourceList.add( new ResourceElement( xid, xaRes ) );
					xaRes.start( xid, XAResource.TMNOFLAGS );
					try
					{
						TxManager.getManager().getTxLog().addBranch( 
							globalId, branchId );
					}
					catch ( IOException e )
					{
						e.printStackTrace();
						log.severe( "Error writing transaction log" );
						TxManager.getManager().setTmNotOk();
						throw new SystemException( "TM encountered a problem, " +
							" error writing transaction log," + e );
					}
					return true;
				}
				Xid sameRmXid = null;
				Iterator<ResourceElement> itr = resourceList.iterator();
				while ( itr.hasNext() )
				{
					ResourceElement re = itr.next();
					if ( sameRmXid == null && 
						re.getResource().isSameRM( xaRes ) )
					{
						sameRmXid = re.getXid();
					}
					if ( xaRes == re.getResource() )
					{
						if ( re.getStatus() == RS_SUSPENDED )
						{
							xaRes.start( re.getXid(), XAResource.TMRESUME );
						}
						else
						{
							// either enlisted or delisted
							// is TMJOIN correct then?
							xaRes.start( re.getXid(), XAResource.TMJOIN );
						}
						re.setStatus( RS_ENLISTED );
						return true;
					}
				}
				if ( sameRmXid != null ) // should we join?
				{
					resourceList.add( 
					 	new ResourceElement( sameRmXid, xaRes ) );
					xaRes.start( sameRmXid, XAResource.TMJOIN );
				}
				else // new branch
				{
					// ResourceElement re = resourceList.getFirst();
					byte branchId[] =
						TxManager.getManager().getBranchId( xaRes ); 
					Xid xid = new XidImpl( globalId, branchId );
					resourceList.add( new ResourceElement( xid, xaRes ) );
					xaRes.start( xid, XAResource.TMNOFLAGS );
					try
					{
						TxManager.getManager().getTxLog().addBranch( 
							globalId, branchId );
					}
					catch ( IOException e )
					{
						e.printStackTrace();
						log.severe( "Error writing transaction log" );
						TxManager.getManager().setTmNotOk();
						throw new SystemException( "TM encountered a problem, " +
							" error writing transaction log," + e );
					}
				}
				return true;
			}
			catch ( XAException e )
			{
				e.printStackTrace();
				log.severe( "Unable to enlist resource[" + xaRes + "]" );
				status = Status.STATUS_MARKED_ROLLBACK;
				return false;
			}
		}
		else if ( status == Status.STATUS_ROLLING_BACK || 
			status == Status.STATUS_ROLLEDBACK || 
			status == Status.STATUS_MARKED_ROLLBACK )
		{
            throw new RollbackException("Tx status is: " + 
				TxManager.getManager().getTxStatusAsString( status ) );
		}
		throw new IllegalStateException( "Tx status is: " + 
			TxManager.getManager().getTxStatusAsString( status ) );
	}

	public synchronized boolean delistResource( XAResource xaRes, int flag )
		throws IllegalStateException
	{
		if ( xaRes == null )
		{
			throw new IllegalArgumentException( "Null xa resource" );
		}
		if ( flag != XAResource.TMSUCCESS && flag != XAResource.TMSUSPEND &&
			flag != XAResource.TMFAIL )
		{
			throw new IllegalArgumentException( "Illegal flag: " + flag );
		}
		ResourceElement re = null;
		Iterator<ResourceElement> itr = resourceList.iterator();
		while ( itr.hasNext() )
		{
			ResourceElement reMatch = itr.next();
			if ( reMatch.getResource() == xaRes )
			{
				re = reMatch;
				break;
			}
		}
		if ( re == null )
		{
			return false;
		}
		if ( status == Status.STATUS_ACTIVE || 
			status == Status.STATUS_MARKED_ROLLBACK )
		{
			try
			{
				xaRes.end( re.getXid(), flag );
				if ( flag == XAResource.TMSUSPEND || 
					flag == XAResource.TMFAIL )
				{
					re.setStatus( RS_SUSPENDED );
				}
				else
				{
					re.setStatus( RS_DELISTED );
				}
				return true;
			}
			catch ( XAException e )
			{
				e.printStackTrace();
				log.severe( "Unable to delist resource[" + xaRes + "]" );
				status = Status.STATUS_MARKED_ROLLBACK;
				return false;
			}
		}
		throw new IllegalStateException( "Tx status is: " + 
			TxManager.getManager().getTxStatusAsString( status ) );
	}

	// TODO: figure out if this needs syncrhonization or make status volatile
	public int getStatus() // throws SystemException
	{
		return status;
	}
	
	void setStatus( int status )
	{
		this.status = status;
	}
	
	private boolean beforeCompletionRunning = false;
	private List<Synchronization> syncHooksAdded = 
		new ArrayList<Synchronization>();

	public synchronized void registerSynchronization( Synchronization s )
		throws RollbackException, IllegalStateException
	{
		if ( s == null )
		{
			throw new IllegalArgumentException( "Null parameter" );
		}
		if ( status == Status.STATUS_ACTIVE || 
			status == Status.STATUS_PREPARING )
		{
			if ( !beforeCompletionRunning )
			{
				syncHooks.add( s );
			}
			else
			{
				// avoid CME if synchronization is added in before completion
				syncHooksAdded.add( s );
			}
		}
		else if ( status == Status.STATUS_ROLLING_BACK || 
			status == Status.STATUS_ROLLEDBACK || 
			status == Status.STATUS_MARKED_ROLLBACK )
		{
            throw new RollbackException("Tx status is: " + 
				TxManager.getManager().getTxStatusAsString( status ) );
		}
		else
		{
			throw new IllegalStateException( "Tx status is: " + 
				TxManager.getManager().getTxStatusAsString( status ) );
		}
	}
	
	synchronized void doBeforeCompletion()
	{
		beforeCompletionRunning = true;
		try
		{
			for ( Synchronization s : syncHooks )
			{
				try
				{
					s.beforeCompletion();
				}
				catch ( Throwable t )
				{
					log.warning( "Caught exception from tx syncronization[" + s + 
						"] beforeCompletion()" );
				}
			}
			// execute any hooks added since we entered doBeforeCompletion
			while ( !syncHooksAdded.isEmpty() )
			{
				List<Synchronization> addedHooks = syncHooksAdded;
				syncHooksAdded = new ArrayList<Synchronization>();
				for ( Synchronization s : addedHooks )
				{
					s.beforeCompletion();
					syncHooks.add( s );
				}
			}
		}
		finally
		{
			beforeCompletionRunning = false;
		}
	}

	synchronized void doAfterCompletion()
	{
		for ( Synchronization s : syncHooks )
		{
			try
			{
				s.afterCompletion( status );
			}
			catch ( Throwable t )
			{
				log.warning( "Caught exception from tx syncronization[" + s + 
					"] afterCompletion()" );
			}
		}
		syncHooks = null; // help gc
	}

	public void setRollbackOnly() throws IllegalStateException
	{
		if ( status == Status.STATUS_ACTIVE || 
			status == Status.STATUS_PREPARING ||
			status == Status.STATUS_PREPARED ||
			status == Status.STATUS_MARKED_ROLLBACK ||
			status == Status.STATUS_ROLLING_BACK )
		{
			status = Status.STATUS_MARKED_ROLLBACK;
		}
		else
		{
			throw new IllegalStateException( "Tx status is: " + 
				TxManager.getManager().getTxStatusAsString( status ) );
		}
	}
	
	public boolean equals( Object o )
	{
		if ( !( o instanceof TransactionImpl ) )
		{
			return false;
		}
		TransactionImpl other = ( TransactionImpl ) o;
		if ( globalId.length != other.globalId.length ) 
		{
			return false;
		}
		for ( int i = 0; i < globalId.length; i++ )
		{
			if ( globalId[i] != other.globalId[i] )
			{
				return false;
			}
		}
		return true;
	}

	private volatile int hashCode = 0;
		
	public int hashCode()
	{
		if ( hashCode == 0 )
		{
			int calcHash = 0;
			for ( int i = 0; i < 4; i++ )
			{
				calcHash += globalId[ globalId.length - i - 1 ] << i * 8;
			}
			hashCode = 3217 * calcHash;
		}
		return hashCode;
	}
	
	int getResourceCount()
	{
		return resourceList.size();
	}
	
	private boolean isOnePhase()
	{
		if ( resourceList.size() == 0 )
		{
			log.severe( "Detected zero resources in resourceList" );
			return true;
		}
		// check for more than one unique xid
		Iterator<ResourceElement> itr = resourceList.iterator();
		Xid xid = itr.next().getXid();
		while ( itr.hasNext() )
		{
			if ( !xid.equals( itr.next().getXid() ) )
			{
				return false;
			}
		}
		return true;
	}
	
	void doCommit() throws XAException, SystemException
	{
		boolean onePhase = isOnePhase();
		boolean readOnly = true;
		if ( !onePhase )
		{
			// prepare
			status = Status.STATUS_PREPARING;
			LinkedList<Xid> preparedXids = new LinkedList<Xid>();
			Iterator<ResourceElement> itr = resourceList.iterator();
			while ( itr.hasNext() )
			{
				ResourceElement re = itr.next();
				if ( !preparedXids.contains( re.getXid() ) )
				{
					preparedXids.add( re.getXid() );
					int vote = re.getResource().prepare( re.getXid() );
					if ( vote == XAResource.XA_OK )
					{
						readOnly = false;
					}
					else if ( vote == XAResource.XA_RDONLY )
					{
						re.setStatus( RS_READONLY );
					}
					else
					{
						// rollback tx
						status = Status.STATUS_MARKED_ROLLBACK;
						return;
					}
				}
				else
				{
					// set it to readonly, only need to commit once
					re.setStatus( RS_READONLY );
				}
			}
			status = Status.STATUS_PREPARED;
		}
		// commit
		if ( !onePhase && readOnly )
		{
			status = Status.STATUS_COMMITTED;
			return;
		}
		if ( !onePhase )
		{
			try
			{
				TxManager.getManager().getTxLog().markAsCommitting( 
					getGlobalId() );
			}
			catch ( IOException e )
			{
				e.printStackTrace();
				log.severe( "Error writing transaction log" );
				TxManager.getManager().setTmNotOk();
				throw new SystemException( "TM encountered a problem, " +
					" error writing transaction log," + e );
			}
		}
		status = Status.STATUS_COMMITTING;
		Iterator<ResourceElement> itr = resourceList.iterator();
		while ( itr.hasNext() )
		{
			ResourceElement re = itr.next();
			if ( re.getStatus() != RS_READONLY )
			{
				re.getResource().commit( re.getXid(), onePhase );
			}
		}
		status = Status.STATUS_COMMITTED;
	}
	
	void doRollback() throws XAException
	{
		status = Status.STATUS_ROLLING_BACK;
		LinkedList<Xid> rolledbackXids = new LinkedList<Xid>();
		Iterator<ResourceElement> itr = resourceList.iterator();
		while ( itr.hasNext() )
		{
			ResourceElement re = itr.next();
			if ( !rolledbackXids.contains( re.getXid() ) )
			{
				rolledbackXids.add( re.getXid() );
				re.getResource().rollback( re.getXid() );
			}
		}
		status = Status.STATUS_ROLLEDBACK;
	}

	private static class ResourceElement
	{
		private Xid xid = null;
		private XAResource resource = null;
		private int status;
		
		ResourceElement( Xid xid, XAResource resource )
		{
			this.xid = xid;
			this.resource = resource;
			status = RS_ENLISTED;
		}
		
		Xid getXid()
		{
			return xid;
		}
		
		XAResource getResource()
		{
			return resource;
		}
		
		int getStatus()
		{
			return status;
		}
		
		void setStatus( int status )
		{
			this.status = status;
		}
		
		public String toString()
		{
			String statusString = null;
			switch ( status )
			{
				case RS_ENLISTED: statusString = "ENLISTED"; break;
				case RS_DELISTED: statusString = "DELISTED"; break;
				case RS_SUSPENDED: statusString = "SUSPENDED"; break;
				case RS_READONLY: statusString = "READONLY"; break;
				default: statusString = "UNKOWN";
			}

			return "Xid[" + xid + "] XAResource[" + resource + "] Status[" +
			 statusString + "]";
		}
	}
}
