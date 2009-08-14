/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
    private static Logger log = Logger.getLogger( TransactionImpl.class
        .getName() );

    private static final int RS_ENLISTED = 0;
    private static final int RS_SUSPENDED = 1;
    private static final int RS_DELISTED = 2;
    private static final int RS_READONLY = 3; // set in prepare

    private final byte globalId[];
    private int status = Status.STATUS_ACTIVE;
    private boolean active = true;
    private boolean globalStartRecordWritten = false;

    private final LinkedList<ResourceElement> resourceList = 
        new LinkedList<ResourceElement>();
    private List<Synchronization> syncHooks = 
        new ArrayList<Synchronization>();

    private final int eventIdentifier;

    private final TxManager txManager;

    TransactionImpl( TxManager txManager )
    {
        this.txManager = txManager;
        globalId = XidImpl.getNewGlobalId();
        eventIdentifier = txManager.getNextEventIdentifier();
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
        StringBuffer txString = new StringBuffer( "Transaction[Status="
            + txManager.getTxStatusAsString( status ) + ",ResourceList=" );
        Iterator<ResourceElement> itr = resourceList.iterator();
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
        txManager.commit();
    }
    
    boolean isGlobalStartRecordWritten()
    {
        return globalStartRecordWritten;
    }

    public synchronized void rollback() throws IllegalStateException,
        SystemException
    {
        // make sure tx not suspended
        txManager.rollback();
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
                    if ( !globalStartRecordWritten )
                    {
                        txManager.writeStartRecord( globalId );
                        globalStartRecordWritten = true;
                    }
                    // 
                    byte branchId[] = txManager.getBranchId( xaRes );
                    Xid xid = new XidImpl( globalId, branchId );
                    resourceList.add( new ResourceElement( xid, xaRes ) );
                    xaRes.start( xid, XAResource.TMNOFLAGS );
                    try
                    {
                        txManager.getTxLog().addBranch( globalId, branchId );
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                        log.severe( "Error writing transaction log" );
                        txManager.setTmNotOk();
                        throw new SystemException( "TM encountered a problem, "
                            + " error writing transaction log," + e );
                    }
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
                    resourceList.add( new ResourceElement( sameRmXid, xaRes ) );
                    xaRes.start( sameRmXid, XAResource.TMJOIN );
                }
                else
                // new branch
                {
                    // ResourceElement re = resourceList.getFirst();
                    byte branchId[] = txManager.getBranchId( xaRes );
                    Xid xid = new XidImpl( globalId, branchId );
                    resourceList.add( new ResourceElement( xid, xaRes ) );
                    xaRes.start( xid, XAResource.TMNOFLAGS );
                    try
                    {
                        txManager.getTxLog().addBranch( globalId, branchId );
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                        log.severe( "Error writing transaction log" );
                        txManager.setTmNotOk();
                        throw new SystemException( "TM encountered a problem, "
                            + " error writing transaction log," + e );
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
            throw new RollbackException( "Tx status is: "
                + txManager.getTxStatusAsString( status ) );
        }
        throw new IllegalStateException( "Tx status is: "
            + txManager.getTxStatusAsString( status ) );
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
                e.printStackTrace();
                log.severe( "Unable to delist resource[" + xaRes + "]" );
                status = Status.STATUS_MARKED_ROLLBACK;
                return false;
            }
        }
        throw new IllegalStateException( "Tx status is: "
            + txManager.getTxStatusAsString( status ) );
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
                    log.warning( "Caught exception from tx syncronization[" + s
                        + "] beforeCompletion()" );
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
                log.warning( "Caught exception from tx syncronization[" + s
                    + "] afterCompletion()" );
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
            throw new IllegalStateException( "Tx status is: "
                + txManager.getTxStatusAsString( status ) );
        }
    }

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
                txManager.getTxLog().markAsCommitting( getGlobalId() );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                log.severe( "Error writing transaction log" );
                txManager.setTmNotOk();
                throw new SystemException( "TM encountered a problem, "
                    + " error writing transaction log," + e );
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

    synchronized void markAsActive()
    {
        if ( active )
        {
            throw new IllegalStateException( "Transaction[" + this
                + "] already active" );
        }
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
}