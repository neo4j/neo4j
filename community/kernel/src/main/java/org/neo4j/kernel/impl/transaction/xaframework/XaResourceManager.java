/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.*;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.CommitNotificationFailedException;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

// make package access?
public class XaResourceManager
{
    private static class ResourceTransaction
    {
        private Xid xid;
        private final XaTransaction xaTx;

        ResourceTransaction( XaTransaction xaTx )
        {
            this.xaTx = xaTx;
        }
    }

    private final ArrayMap<XAResource,ResourceTransaction> xaResourceMap = new ArrayMap<>();
    private final ArrayMap<Xid,XidStatus> xidMap = new ArrayMap<>();
    private final TransactionMonitor transactionMonitor;
    private int recoveredTxCount = 0;
    private final Map<Integer, TransactionInfo> recoveredTransactions = new HashMap<>();

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
        this.msgLog = log.getStringLogger();
    }

    /**
     * Creates a transaction that can be used for read operations, but is not yet
     * {@link #start(XAResource, Xid) started} and hasn't got an identifier associated with it.
     * A call to {@link #start(XAResource, Xid)} after a call to this method will start the transaction
     * created here. Otherwise if there's no {@link #createTransaction(XAResource)} call prior to a
     * {@link #start(XAResource, Xid)} call the transaction will be created there instead.
     *
     * @param xaResource the {@link XAResource} to create the transaction for.
     * @return the created transaction.
     * @throws XAException if the {@code resource} was already associated with another transaction.
     */
    synchronized XaTransaction createTransaction( XAResource xaResource )
            throws XAException
    {
        if ( xaResourceMap.get( xaResource ) != null )
        {
            throw new XAException( "Resource[" + xaResource + "] already enlisted or suspended" );
        }

        XaTransaction xaTx = tf.create( dataSource.getLastCommittedTxId(), transactionManager.getTransactionState() );
        xaResourceMap.put( xaResource, new ResourceTransaction( xaTx ) );
        return xaTx;
    }

    synchronized XaTransaction getXaTransaction( XAResource xaRes )
            throws XAException
    {
        XidStatus status = xidMap.get( xaResourceMap.get( xaRes ).xid );
        if ( status == null )
        {
            throw new XAException( "Resource[" + xaRes + "] not enlisted" );
        }
        return status.getTransactionStatus().getTransaction();
    }

    synchronized void start( XAResource xaResource, Xid xid )
            throws XAException
    {
        ResourceTransaction tx = xaResourceMap.get( xaResource );
        if ( tx == null )
        {
            // Why allow creating the transaction here? See javadoc about createTransaction.
            createTransaction( xaResource );
            tx = xaResourceMap.get( xaResource );
        }

        if ( xidMap.get( xid ) == null ) // TODO why are we allowing this?
        {
            int identifier = log.start( xid, txIdGenerator.getCurrentMasterId(), txIdGenerator.getMyId(),
                    dataSource.getLastCommittedTxId() );
            tx.xaTx.setIdentifier( identifier );
            xidMap.put( xid, new XidStatus( tx.xaTx ) );
            tx.xid = xid;
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

        ResourceTransaction tx = new ResourceTransaction( null /* TODO hmm */ );
        tx.xid = xid;
        xaResourceMap.put( xaResource, tx );
    }

    synchronized void end( XAResource xaResource, Xid xid ) throws XAException
    {
        if ( xaResourceMap.remove( xaResource ) == null )
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
        XidStatus xidStatus = xidMap.get( xid );
        if ( xidStatus == null )
        {
            throw new XAException( "Unknown xid[" + xid + "]" );
        }
        if ( xaResourceMap.remove( xaResource ) == null )
        {
            throw new XAException( "Resource[" + xaResource + "] not enlisted" );
        }
        xidStatus.getTransactionStatus().markAsRollback();
    }

    synchronized void validate( XAResource xaResource ) throws XAException
    {
        ResourceTransaction tx = xaResourceMap.get( xaResource );
        XidStatus status = null;
        if ( tx == null || (status = xidMap.get( tx.xid )) == null )
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
            // Called here to release locks of two-phase read-only transactions
            // cf. TransactionImpl.doCommit() and commit()
            commitKernelTx( xaTransaction );
            log.done( xaTransaction.getIdentifier() );
            xidMap.remove( xid );
            if ( xaTransaction.isRecovered() )
            {
                oneMoreTransactionRecovered();
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

    private void oneMoreTransactionRecovered()
    {
        recoveredTxCount--;
        checkIfRecoveryComplete();
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
                oneMoreTransactionRecovered();
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
    synchronized void injectOnePhaseCommit( Xid xid, LockGroup lockGroup ) throws XAException
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
        xaTransaction.commit( lockGroup );
        transactionMonitor.injectOnePhaseCommit( xid );
    }

    synchronized void injectTwoPhaseCommit( Xid xid, LockGroup lockGroup ) throws XAException
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
        xaTransaction.commit( lockGroup );
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

        try ( LockGroup lockGroup = new LockGroup() )
        {
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
                isReadOnly = xaTransaction.isReadOnly();

                if ( isReadOnly )
                {
                    // called for one-phase read-only transactions since they skip prepare
                    // cf. TransactionImpl.doCommit() and prepare()
                    commitReadTx( xid, onePhase, xaTransaction, txStatus );
                }
                else
                {
                    commitWriteTx( lockGroup, xid, onePhase, xaTransaction, txStatus, txIdGenerator );
                }
            }
            commitKernelTx( xaTransaction );
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

    private void commitReadTx( Xid xid, boolean onePhase, XaTransaction xaTransaction,
                               TransactionStatus txStatus ) throws XAException
    {
        if ( onePhase )
        {
            txStatus.markAsPrepared();
        }
        if ( !txStatus.prepared() || txStatus.rollback() )
        {
            throw new XAException( "Transaction not prepared or "
                    + "(marked as) rolledbacked" );
        }

        if ( !xaTransaction.isRecovered() )
        {
            log.forget( xaTransaction.getIdentifier() );
        }

        xidMap.remove( xid );
        if ( xaTransaction.isRecovered() )
        {
            oneMoreTransactionRecovered();
        }
    }

    private void commitWriteTx( LockGroup lockGroup, Xid xid, boolean onePhase, XaTransaction xaTransaction,
                                TransactionStatus txStatus, TxIdGenerator txIdGenerator ) throws XAException
    {
        checkStartWritten( txStatus, xaTransaction );

        if ( onePhase )
        {
            txStatus.markAsPrepared();
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

        if ( !txStatus.prepared() || txStatus.rollback() )
        {
            throw new XAException( "Transaction not prepared or "
                    + "(marked as) rolledbacked" );
        }

        if ( !onePhase && !xaTransaction.isRecovered() )
        {
            long txId = txIdGenerator.generate( dataSource,
                    xaTransaction.getIdentifier() );
            xaTransaction.setCommitTxId( txId );
            // The call to getForceMode() is critical for correctness.
            // See TxManager.getTransaction() for details.
            log.commitTwoPhase( xaTransaction.getIdentifier(),
                    xaTransaction.getCommitTxId(), getForceMode() );
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

        xaTransaction.commit( lockGroup );

        if ( !xaTransaction.isRecovered() )
        {
            log.done( xaTransaction.getIdentifier() );
        }
        else if ( !log.scanIsComplete() || recoveredTxCount > 0 )
        {
            int identifier = xaTransaction.getIdentifier();
            Start startEntry = log.getStartEntry( identifier );
            recoveredTransactions.put( identifier, new TransactionInfo( identifier, onePhase,
                    xaTransaction.getCommitTxId(), startEntry.getMasterId(), startEntry.getChecksum() ) );
        }

        xidMap.remove( xid );

        if ( xaTransaction.isRecovered() )
        {
            oneMoreTransactionRecovered();
        }
        transactionMonitor.transactionCommitted( xid, xaTransaction.isRecovered() );
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
        rollbackKernelTx( xaTransaction );

        log.done( xaTransaction.getIdentifier() );
        xidMap.remove( xid );
        if ( xaTransaction.isRecovered() )
        {
            oneMoreTransactionRecovered();
        }

        return txStatus.getTransaction();
    }

    synchronized XaTransaction forget( Xid xid ) throws XAException
    {
        XidStatus status = xidMap.get( xid );
        if ( status == null )
        {
            // START record has not been applied,
            // so we don't have a transaction
            return null;
        }
        TransactionStatus txStatus = status.getTransactionStatus();
        XaTransaction xaTransaction = txStatus.getTransaction();

        if(!xaTransaction.isReadOnly())
        {
            checkStartWritten( txStatus, xaTransaction );
            log.done( xaTransaction.getIdentifier() );
        }
        xidMap.remove( xid );
        if ( xaTransaction.isRecovered() )
        {
            oneMoreTransactionRecovered();
        }
        return xaTransaction;
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
            oneMoreTransactionRecovered();
        }
    }

    synchronized void checkXids() throws IOException
    {
        msgLog.logMessage( "XaResourceManager[" + name + "] sorting " +
                xidMap.size() + " xids" );
        Iterator<Xid> keyIterator = xidMap.keySet().iterator();
        LinkedList<Xid> xids = new LinkedList<>();
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
                for ( TransactionInfo recoveredTx : sortByTxId( recoveredTransactions.values() ) )
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

    private Iterable<TransactionInfo> sortByTxId( Collection<TransactionInfo> set )
    {
        List<TransactionInfo> list = new ArrayList<>( set );
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

    private void commitKernelTx( XaTransaction xaTransaction ) throws XAException
    {
        if ( !(xaTransaction instanceof NeoStoreTransaction) )
        {
            return;
        }
        try
        {
            NeoStoreTransaction neoStoreTransaction = (NeoStoreTransaction)xaTransaction;
            neoStoreTransaction.commitChangesToCache();
            neoStoreTransaction.kernelTransaction().commit();
        }
        catch ( TransactionFailureException e )
        {
            throw e.unBoxedForCommit();
        }
    }

    private void rollbackKernelTx( XaTransaction xaTransaction ) throws XAException
    {
        // Hack until the WriteTx/KernelTx structure is sorted out
        if ( !(xaTransaction instanceof NeoStoreTransaction) )
        {
            return;
        }
        try
        {
            ((NeoStoreTransaction)xaTransaction).kernelTransaction().rollback();
        }
        catch ( TransactionFailureException e )
        {
            throw e.unBoxedForCommit();
        }
    }
}
