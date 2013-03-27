/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.util.MultipleCauseException;
import org.neo4j.kernel.impl.util.StringLogger;

class TransactionImpl implements Transaction
{
    private static final int RS_ENLISTED = 0;
    private static final int RS_SUSPENDED = 1;
    private static final int RS_DELISTED = 2;
    private static final int RS_READONLY = 3; // set in prepare

    private final byte globalId[];
    private int status = Status.STATUS_ACTIVE;
    private volatile boolean active = true;
    private boolean globalStartRecordWritten = false;

    private final LinkedList<ResourceElement> resourceList =
        new LinkedList<ResourceElement>();
    private List<Synchronization> syncHooks =
        new ArrayList<Synchronization>();
    private boolean hasChanges;

    private final int eventIdentifier;

    private final TxManager txManager;
    private final StringLogger logger;
    private final ForceMode forceMode;
    private Thread owner;

    private final TransactionState state;
    private TransactionContext transactionContext;

    TransactionImpl( TxManager txManager, ForceMode forceMode, TransactionStateFactory stateFactory, StringLogger logger )
    {
        this.txManager = txManager;
        this.logger = logger;
        this.state = stateFactory.create( this );
        globalId = XidImpl.getNewGlobalId();
        eventIdentifier = txManager.getNextEventIdentifier();
        this.forceMode = forceMode;
        owner = Thread.currentThread();
    }

    /**
     * @return The event identifier for this transaction, a unique per database
     *         run identifier among all transactions initiated by the
     *         transaction manager. Currently an increasing natural number.
     */
    Integer getEventIdentifier()
    {
        return eventIdentifier;
    }

    byte[] getGlobalId()
    {
        return globalId;
    }
    
    boolean hasChanges()
    {
        return hasChanges;
    }
    
    public TransactionState getState()
    {
        return state;
    }

    @Override
    public String toString()
    {

        return String.format( "Transaction(%d, owner:\"%s\")[%s,Resources=%d]",
                eventIdentifier, owner.getName(), txManager.getTxStatusAsString( status ), resourceList.size() );
    }

    @Override
	public synchronized void commit() throws RollbackException,
        HeuristicMixedException, HeuristicRollbackException,
        IllegalStateException, SystemException
    {
        // make sure tx not suspended
        txManager.commit();
        transactionContext.commit();
    }

    boolean isGlobalStartRecordWritten()
    {
        return globalStartRecordWritten;
    }

    @Override
	public synchronized void rollback() throws IllegalStateException,
        SystemException
    {
        // make sure tx not suspended
        txManager.rollback();
        transactionContext.rollback();
    }

    @Override
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
                    if ( !globalStartRecordWritten )
                    {
                        txManager.writeStartRecord( globalId );
                        globalStartRecordWritten = true;
                    }
                    //
                    byte branchId[] = txManager.getBranchId( xaRes );
                    Xid xid = new XidImpl( globalId, branchId );
                    resourceList.add( new ResourceElement( xid, xaRes ) );
                    hasChanges = true;
                    xaRes.start( xid, XAResource.TMNOFLAGS );
                    try
                    {
                        txManager.getTxLog().addBranch( globalId, branchId );
                    }
                    catch ( IOException e )
                    {
                        logger.error( "Error writing transaction log", e );
                        txManager.setTmNotOk( e );
                        throw Exceptions.withCause( new SystemException( "TM encountered a problem, "
                                                                         + " error writing transaction log" ), e );
                    }
                    // TODO ties HA to our TxManager
                    if ( !hasAnyLocks() )
                        getState().getTxHook().initializeTransaction( eventIdentifier );
                    return true;
                }
                Xid sameRmXid = null;
                Iterator<ResourceElement> itr = resourceList.iterator();
                while ( itr.hasNext() )
                {
                    ResourceElement re = itr.next();
                    if ( sameRmXid == null && re.getResource().isSameRM( xaRes ) )
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
                    addResourceToList( sameRmXid, xaRes );
                    xaRes.start( sameRmXid, XAResource.TMJOIN );
                }
                else
                // new branch
                {
                    // ResourceElement re = resourceList.getFirst();
                    byte branchId[] = txManager.getBranchId( xaRes );
                    Xid xid = new XidImpl( globalId, branchId );
                    addResourceToList( xid, xaRes );
                    xaRes.start( xid, XAResource.TMNOFLAGS );
                    try
                    {
                        txManager.getTxLog().addBranch( globalId, branchId );
                    }
                    catch ( IOException e )
                    {
                        logger.error( "Error writing transaction log", e );
                        txManager.setTmNotOk( e );
                        throw Exceptions.withCause( new SystemException( "TM encountered a problem, "
                                                                         + " error writing transaction log" ), e );
                    }
                }
                return true;
            }
            catch ( XAException e )
            {
                logger.error( "Unable to enlist resource[" + xaRes + "]", e );
                status = Status.STATUS_MARKED_ROLLBACK;
                return false;
            }
        }
        else if ( status == Status.STATUS_ROLLING_BACK ||
            status == Status.STATUS_ROLLEDBACK ||
            status == Status.STATUS_MARKED_ROLLBACK )
        {
            throw new RollbackException( "Tx status is: "
                + txManager.getTxStatusAsString( status ) );
        }
        throw new IllegalStateException( "Tx status is: "
            + txManager.getTxStatusAsString( status ) );
    }

    private void addResourceToList( Xid xid, XAResource xaRes )
    {
        ResourceElement element = new ResourceElement( xid, xaRes );
        if ( Arrays.equals( NeoStoreXaDataSource.BRANCH_ID, xid.getBranchQualifier() ) )
        {
            resourceList.addFirst( element );
        }
        else
        {
            resourceList.add( element );
        }
        hasChanges = true;
    }

    @Override
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
                if ( flag == XAResource.TMSUSPEND || flag == XAResource.TMFAIL )
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
                logger.error( "Unable to delist resource[" + xaRes + "]", e );
                status = Status.STATUS_MARKED_ROLLBACK;
                return false;
            }
        }
        throw new IllegalStateException( "Tx status is: "
            + txManager.getTxStatusAsString( status ) );
    }

    // TODO: figure out if this needs syncrhonization or make status volatile
    @Override
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

    @Override
	public synchronized void registerSynchronization( Synchronization s )
        throws RollbackException, IllegalStateException
    {
        if ( s == null )
        {
            throw new IllegalArgumentException( "Null parameter" );
        }
        if ( status == Status.STATUS_ACTIVE ||
            status == Status.STATUS_PREPARING ||
            status == Status.STATUS_MARKED_ROLLBACK )
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
            status == Status.STATUS_ROLLEDBACK )
        {
            throw new RollbackException( "Tx status is: "
                + txManager.getTxStatusAsString( status ) );
        }
        else
        {
            throw new IllegalStateException( "Tx status is: "
                + txManager.getTxStatusAsString( status ) );
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
                    addRollbackCause(t);
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
                logger.warn( "Caught exception from tx syncronization[" + s
                        + "] afterCompletion()", t );
            }
        }
        syncHooks = null; // help gc
    }

    @Override
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
            throw new IllegalStateException( "Tx status is: "
                + txManager.getTxStatusAsString( status ) );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !(o instanceof TransactionImpl) )
        {
            return false;
        }
        TransactionImpl other = (TransactionImpl) o;
        return this.eventIdentifier == other.eventIdentifier;
    }

    private volatile int hashCode = 0;

	private Throwable rollbackCause;

    @Override
    public int hashCode()
    {
        if ( hashCode == 0 )
        {
            hashCode = 3217 * eventIdentifier;
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
            logger.warn( "Detected zero resources in resourceList" );
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
                txManager.getTxLog().markAsCommitting( getGlobalId(), forceMode );
            }
            catch ( IOException e )
            {
                logger.error( "Error writing transaction log", e );
                txManager.setTmNotOk( e );
                throw Exceptions.withCause( new SystemException( "TM encountered a problem, "
                                                                 + " error writing transaction log" ), e );
            }
        }
        status = Status.STATUS_COMMITTING;
        Iterator<ResourceElement> itr = resourceList.iterator();
        while ( itr.hasNext() )
        {
            ResourceElement re = itr.next();
            if ( re.getStatus() != RS_READONLY )
            {
                try
                {
                    re.getResource().commit( re.getXid(), onePhase );
                } catch(XAException e)
                {
                    throw e;
                } catch(Throwable e)
                {
                    throw Exceptions.withCause( new XAException(XAException.XAER_RMERR), e );
                }
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

    public StatementContext newStatementContext()
    {
        return transactionContext.newStatementContext();
    }

    /*
     * There is a circular dependency between creating the transaction context and this
     * transactionimpl. As we move forward with introducing the KernelAPI, having TransactionImpl
     * and TxManager handle TransactionContext like this should be refactored and moved to a point
     * where the Neo4j transaction handling lives cleanly under the Kernel API, and is not tied to
     * threads. The thread-bound, JTA-based, transaction management should probably live outside
     * the kernel API entirely.
     *
     * However, in the spirit of baby steps, we hook into the current tx infrastructure at a few
     * points to not have to do the full move in one step.
     */
    public void setTransactionContext( TransactionContext transactionContext )
    {
        this.transactionContext = transactionContext;
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

        @Override
        public String toString()
        {
            String statusString = null;
            switch ( status )
            {
                case RS_ENLISTED:
                    statusString = "ENLISTED";
                    break;
                case RS_DELISTED:
                    statusString = "DELISTED";
                    break;
                case RS_SUSPENDED:
                    statusString = "SUSPENDED";
                    break;
                case RS_READONLY:
                    statusString = "READONLY";
                    break;
                default:
                    statusString = "UNKNOWN";
            }

            return "Xid[" + xid + "] XAResource[" + resource + "] Status["
                + statusString + "]";
        }
    }

    boolean isActive()
    {
        return active;
    }

    synchronized void markAsActive()
    {
        if ( active )
        {
            throw new IllegalStateException( "Transaction[" + this
                + "] already active" );
        }
        owner = Thread.currentThread();
        active = true;
    }

    synchronized void markAsSuspended()
    {
        if ( !active )
        {
            throw new IllegalStateException( "Transaction[" + this
                + "] already suspended" );
        }
        active = false;
    }
    
    public ForceMode getForceMode()
    {
        return forceMode;
    }

    public Throwable getRollbackCause()
    {
        return rollbackCause;
    }

    private void addRollbackCause( Throwable cause )
    {
        if ( rollbackCause == null )
        {
            rollbackCause = cause;
        }
        else
        {
            if ( !(rollbackCause instanceof MultipleCauseException) )
            {
                rollbackCause = new MultipleCauseException(
                        "Multiple exceptions occurred, stack traces of all of them available below, or via #getCauses().",
                        rollbackCause );
            }
            ((MultipleCauseException) rollbackCause).addCause( cause );
        }
    }

    public boolean hasAnyLocks()
    {
        return getState().getTxHook().hasAnyLocks( this );
    }

    public void finish( boolean successful )
    {
        getState().getTxHook().finishTransaction( getEventIdentifier(), successful );
    }
}