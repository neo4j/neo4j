/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.CommitNotificationFailedException;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

// make package access?
public class XaResourceManager
{
    private final ArrayMap<XAResource,Xid> xaResourceMap =
        new ArrayMap<XAResource,Xid>();
    private final ArrayMap<Xid,XidStatus> xidMap =
        new ArrayMap<Xid,XidStatus>();
    private final TransactionMonitor transactionMonitor;
    private int recoveredTxCount = 0;
    private final Set<TransactionInfo> recoveredTransactions = new HashSet<TransactionInfo>();

    private XaLogicalLog log = null;
    private final XaTransactionFactory tf;
    private final String name;
    private final TxIdGenerator txIdGenerator;
    private final XaDataSource dataSource;
    private StringLogger msgLog;
    private final AbstractTransactionManager transactionManager;
    private final RecoveryVerifier recoveryVerifier;

    public XaResourceManager( XaDataSource dataSource, XaTransactionFactory tf,
            TxIdGenerator txIdGenerator, AbstractTransactionManager transactionManager,
            RecoveryVerifier recoveryVerifier, String name, Monitors monitors )
    {
        this.dataSource = dataSource;
        this.tf = tf;
        this.txIdGenerator = txIdGenerator;
        this.transactionManager = transactionManager;
        this.recoveryVerifier = recoveryVerifier;
        this.name = name;
        this.transactionMonitor = monitors.newMonitor( TransactionMonitor.class, getClass(), dataSource.getName() );
    }

    public synchronized void setLogicalLog( XaLogicalLog log )
    {
        this.log = log;
        if ( log != null )
        {
            this.msgLog = log.getStringLogger();
        }
        else
        {
            this.msgLog = StringLogger.SYSTEM;
        }
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
            throw new XAException( "Resource[" + xaResource
                + "] already enlisted or suspended" );
        }
        xaResourceMap.put( xaResource, xid );
        if ( xidMap.get( xid ) == null )
        {
            int identifier = log.start( xid, txIdGenerator.getCurrentMasterId(), txIdGenerator.getMyId() );
            XaTransaction xaTx = tf.create( identifier, transactionManager.getTransactionState() );
            xidMap.put( xid, new XidStatus( xaTx ) );
        }
    }

    synchronized void injectStart( Xid xid, XaTransaction tx )
        throws IOException
    {
        if ( xidMap.get( xid ) != null )
        {
            throw new IOException( "Inject start failed, xid: " + xid
                + " already injected" );
        }
        xidMap.put( xid, new XidStatus( tx ) );
        recoveredTxCount++;
    }

    synchronized void resume( Xid xid ) throws XAException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            throw new XAException( "Unknown xid[" + xid + "]" );
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
            throw new XAException( "Unknown xid[" + xid + "]" );
        }
        if ( xaResourceMap.get( xaResource ) != null )
        {
            throw new XAException( "Resource[" + xaResource
                + "] already enlisted" );
        }
        xaResourceMap.put( xaResource, xid );
    }

    synchronized void end( XAResource xaResource, Xid xid ) throws XAException
    {
        Xid xidEntry = xaResourceMap.remove( xaResource );
        if ( xidEntry == null )
        {
            throw new XAException( "Resource[" + xaResource + "] not enlisted" );
        }
    }

    synchronized void suspend( Xid xid ) throws XAException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            throw new XAException( "Unknown xid[" + xid + "]" );
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
            throw new XAException( "Unknown xid[" + xid + "]" );
        }
        Xid xidEntry = xaResourceMap.remove( xaResource );
        if ( xidEntry == null )
        {
            throw new XAException( "Resource[" + xaResource + "] not enlisted" );
        }
        XidStatus status = xidMap.get( xid );
        status.getTransactionStatus().markAsRollback();
    }

    synchronized void validate( XAResource xaResource ) throws XAException
    {
        XidStatus status = xidMap.get( xaResourceMap.get( xaResource ) );
        if ( status == null )
        {
            throw new XAException( "Resource[" + xaResource + "] not enlisted" );
        }
        if ( !status.getActive() )
        {
            throw new XAException( "Resource[" + xaResource + "] suspended" );
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
        private final TransactionStatus txStatus;

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

    private static class TransactionStatus implements Comparable<TransactionStatus>
    {
        private boolean prepared = false;
        private boolean commitStarted = false;
        private boolean rollback = false;
        private boolean startWritten = false;
        private final XaTransaction xaTransaction;
        private int sequenceNumber;

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

        boolean startWritten()
        {
            return startWritten;
        }

        void markStartWritten()
        {
            this.startWritten = true;
        }

        XaTransaction getTransaction()
        {
            return xaTransaction;
        }

        @Override
        public String toString()
        {
            return "TransactionStatus[" + xaTransaction.getIdentifier()
                + ", prepared=" + prepared + ", commitStarted=" + commitStarted
                + ", rolledback=" + rollback + "]";
        }

        public void setSequenceNumber( int sequenceNumber )
        {
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public int compareTo( TransactionStatus that )
        {
            return this.sequenceNumber > that.sequenceNumber ? 1
                 : this.sequenceNumber < that.sequenceNumber ? -1 : 0;
        }
    }

    private void checkStartWritten( TransactionStatus status, XaTransaction tx )
            throws XAException
    {
        if ( !status.startWritten() && !tx.isRecovered() )
        {
            log.writeStartEntry( tx.getIdentifier() );
            status.markStartWritten();
        }
    }

    synchronized int prepare( Xid xid ) throws XAException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            throw new XAException( "Unknown xid[" + xid + "]" );
        }
        TransactionStatus txStatus = status.getTransactionStatus();
        XaTransaction xaTransaction = txStatus.getTransaction();
        checkStartWritten( txStatus, xaTransaction );
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
            throw new IOException( "Unknown xid[" + xid + "]" );
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
            txStatus.setSequenceNumber( nextTxOrder++ );
            txStatus.markAsPrepared();
            return false;
        }
    }

    private int nextTxOrder = 0;

    // called during recovery
    // if not read only transaction will be commited.
    synchronized void injectOnePhaseCommit( Xid xid ) throws XAException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            throw new XAException( "Unknown xid[" + xid + "]" );
        }
        TransactionStatus txStatus = status.getTransactionStatus();
        txStatus.setSequenceNumber( nextTxOrder++ );
        txStatus.markAsPrepared();
        txStatus.markCommitStarted();
        XaTransaction xaTransaction = txStatus.getTransaction();
        xaTransaction.commit();
        transactionMonitor.injectOnePhaseCommit( xid );
    }

    synchronized void injectTwoPhaseCommit( Xid xid ) throws XAException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            throw new XAException( "Unknown xid[" + xid + "]" );
        }
        TransactionStatus txStatus = status.getTransactionStatus();
        txStatus.setSequenceNumber( nextTxOrder++ );
        txStatus.markAsPrepared();
        txStatus.markCommitStarted();
        XaTransaction xaTransaction = txStatus.getTransaction();
        xaTransaction.commit();
        transactionMonitor.injectTwoPhaseCommit( xid );
    }

    synchronized XaTransaction getXaTransaction( Xid xid ) throws XAException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            throw new XAException( "Unknown xid[" + xid + "]" );
        }
        TransactionStatus txStatus = status.getTransactionStatus();
        XaTransaction xaTransaction = txStatus.getTransaction();
        return xaTransaction;
    }

    /*synchronized(this) in the method*/ XaTransaction commit( Xid xid, boolean onePhase )
        throws XAException
    {
        XaTransaction xaTransaction;
        boolean isReadOnly;

        synchronized ( this )
        {
            XidStatus status = xidMap.get( xid );
            if ( status == null )
            {
                throw new XAException( "Unknown xid[" + xid + "]" );
            }
            TransactionStatus txStatus = status.getTransactionStatus();
            xaTransaction = txStatus.getTransaction();
            TxIdGenerator txIdGenerator = xaTransaction.getTxIdGenerator();
            checkStartWritten( txStatus, xaTransaction );
            isReadOnly = xaTransaction.isReadOnly();
            if ( onePhase )
            {
                txStatus.markAsPrepared();
                if ( !isReadOnly )
                {
                    if ( !xaTransaction.isRecovered() )
                    {
                        xaTransaction.prepare();

                        long txId = txIdGenerator.generate( dataSource,
                                xaTransaction.getIdentifier() );
                        xaTransaction.setCommitTxId( txId );
                        // The call to getForceMode() is critical for correctness.
                        // See TxManager.getTransaction() for details.
                        log.commitOnePhase( xaTransaction.getIdentifier(),
                                xaTransaction.getCommitTxId(), getForceMode() );
                    }
                }
            }
            if ( !txStatus.prepared() || txStatus.rollback() )
            {
                throw new XAException( "Transaction not prepared or "
                    + "(marked as) rolledbacked" );
            }
            if ( !isReadOnly )
            {
                if ( !xaTransaction.isRecovered() )
                {
                    if ( !onePhase )
                    {
                        long txId = txIdGenerator.generate( dataSource,
                                xaTransaction.getIdentifier() );
                        xaTransaction.setCommitTxId( txId );
                        // The call to getForceMode() is critical for correctness.
                        // See TxManager.getTransaction() for details.
                        log.commitTwoPhase( xaTransaction.getIdentifier(),
                                xaTransaction.getCommitTxId(), getForceMode() );
                    }
                }
                txStatus.markCommitStarted();
                if ( xaTransaction.isRecovered() && xaTransaction.getCommitTxId() == -1 )
                {
                    boolean previousRecoveredValue = dataSource.setRecovered( true );
                    try
                    {
                        xaTransaction.setCommitTxId( dataSource.getLastCommittedTxId() + 1 );
                    }
                    finally
                    {
                        dataSource.setRecovered( previousRecoveredValue );
                    }
                }
                xaTransaction.commit();
            }
            if ( !xaTransaction.isRecovered() )
            {
                log.done( xaTransaction.getIdentifier() );
            }
            else if ( !log.scanIsComplete() || recoveredTxCount > 0 )
            {
                int identifier = xaTransaction.getIdentifier();
                Start startEntry = log.getStartEntry( identifier );
                recoveredTransactions.add( new TransactionInfo( identifier, onePhase,
                        xaTransaction.getCommitTxId(), startEntry.getMasterId(), startEntry.getChecksum() ) );
            }
            xidMap.remove( xid );
            if ( xaTransaction.isRecovered() )
            {
                recoveredTxCount--;
                checkIfRecoveryComplete();
            }
            transactionMonitor.transactionCommitted( xid, xaTransaction.isRecovered() );
        }

        if ( !xaTransaction.isRecovered() && !isReadOnly )
        {
            try
            {
                txIdGenerator.committed( dataSource, xaTransaction.getIdentifier(), xaTransaction.getCommitTxId(), null );
            }
            catch ( Exception e )
            {
                throw new CommitNotificationFailedException( e );
            }
        }
        return xaTransaction;
    }

    private ForceMode getForceMode()
    {
        return transactionManager.getForceMode();
    }

    synchronized XaTransaction rollback( Xid xid ) throws XAException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            throw new XAException( "Unknown xid[" + xid + "]" );
        }
        TransactionStatus txStatus = status.getTransactionStatus();
        XaTransaction xaTransaction = txStatus.getTransaction();
        checkStartWritten( txStatus, xaTransaction );
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
        if ( status == null )
            // START record has not been applied,
            // so we don't have a transaction
            return null;
        TransactionStatus txStatus = status.getTransactionStatus();
        XaTransaction xaTransaction = txStatus.getTransaction();
        checkStartWritten( txStatus, xaTransaction );
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
            throw new XAException( "Unknown xid[" + xid + "]" );
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
        return xids.toArray( new Xid[xids.size()] );
    }

    // called from neostore internal recovery
    synchronized void pruneXid( Xid xid ) throws IOException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            throw new IOException( "Unknown xid[" + xid + "]" );
        }
        TransactionStatus txStatus = status.getTransactionStatus();
        XaTransaction xaTransaction = txStatus.getTransaction();
        xidMap.remove( xid );
        if ( xaTransaction.isRecovered() )
        {
            recoveredTransactions.remove( xaTransaction.getIdentifier() );
            recoveredTxCount--;
            checkIfRecoveryComplete();
        }
    }

    synchronized void pruneXidIfExist( Xid xid ) throws IOException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            return;
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
        msgLog.logMessage( "XaResourceManager[" + name + "] sorting " +
                xidMap.size() + " xids" );
        Iterator<Xid> keyIterator = xidMap.keySet().iterator();
        LinkedList<Xid> xids = new LinkedList<Xid>();
        while ( keyIterator.hasNext() )
        {
            xids.add( keyIterator.next() );
        }
        // comparator only used here
        Collections.sort( xids, new Comparator<Xid>()
        {
            @Override
            public int compare( Xid o1, Xid o2 )
            {
                TransactionStatus a = xidMap.get( o1 ).txStatus;
                TransactionStatus b = xidMap.get( o2 ).txStatus;
                return a.compareTo( b );
            }
        } );
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
                    msgLog.debug( "Marking 1PC [" + name + "] tx "
                            + identifier + " as done" );
                    log.doneInternal( identifier );
                    xidMap.remove( xid );
                    recoveredTxCount--;
                }
                else if ( !txStatus.prepared() )
                {
                    msgLog.debug( "Rolling back non prepared tx [" + name + "]"
                            + "txIdent[" + identifier + "]" );
                    log.doneInternal( xaTransaction.getIdentifier() );
                    xidMap.remove( xid );
                    recoveredTxCount--;
                }
                else
                {
                    msgLog.debug( "2PC tx [" + name + "] " + txStatus +
                            " txIdent[" + identifier + "]" );
                }
            }
        }
        checkIfRecoveryComplete();
    }

    private void checkIfRecoveryComplete()
    {
        if ( log.scanIsComplete() && recoveredTxCount == 0 )
        {
            msgLog.logMessage( "XaResourceManager[" + name + "] checkRecoveryComplete " + xidMap.size() + " xids" );
            // log.makeNewLog();
            tf.recoveryComplete();
            try
            {
                for ( TransactionInfo recoveredTx : sortByTxId( recoveredTransactions ) )
                {
                    if ( recoveryVerifier != null && !recoveryVerifier.isValid( recoveredTx ) )
                    {
                        throw new RecoveryVerificationException( recoveredTx.getIdentifier(), recoveredTx.getTxId() );
                    }

                    if ( !recoveredTx.isOnePhase() )
                    {
                        log.commitTwoPhase( recoveredTx.getIdentifier(), recoveredTx.getTxId(), ForceMode.forced );
                    }
                    log.doneInternal( recoveredTx.getIdentifier() );
                }
                recoveredTransactions.clear();
            }
            catch ( IOException e )
            {
                // TODO Why only printStackTrace?
                e.printStackTrace();
            }
            catch ( XAException e )
            {
                // TODO Why only printStackTrace?
                e.printStackTrace();
            }
            msgLog.logMessage( "XaResourceManager[" + name + "] recovery completed." );
        }
    }

    private Iterable<TransactionInfo> sortByTxId( Set<TransactionInfo> set )
    {
        List<TransactionInfo> list = new ArrayList<TransactionInfo>( set );
        Collections.sort( list );
        return list;
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
     * is useful to invoke after the logical log has been opened to detirmine if
     * there are any recovered transactions waiting for the TM to tell them what
     * to do.
     *
     * @return True if recovered transactions exist
     */
    public boolean hasRecoveredTransactions()
    {
        return recoveredTxCount > 0;
    }

    public synchronized void applyCommittedTransaction(
            ReadableByteChannel transaction, long txId ) throws IOException
    {
        long lastCommittedTxId = dataSource.getLastCommittedTxId();
        if ( lastCommittedTxId + 1 == txId )
        {
            log.applyTransaction( transaction );
        }
        else if ( lastCommittedTxId + 1 < txId )
        {
            throw new IOException( "Tried to apply transaction with txId=" + txId +
                    " but last committed txId=" + lastCommittedTxId );
        }
    }

    public synchronized long applyPreparedTransaction(
            ReadableByteChannel transaction ) throws IOException
    {
        try
        {
            long txId = TxIdGenerator.DEFAULT.generate( dataSource, 0 );

            log.applyTransactionWithoutTxId( transaction, txId, getForceMode() );
            return txId;
        }
        catch ( XAException e )
        {
            throw new RuntimeException( e );
        }
    }

    public synchronized long rotateLogicalLog() throws IOException
    {
        return log.rotate();
    }

    XaDataSource getDataSource()
    {
        return dataSource;
    }
}
