/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Default transaction manager implementation
 */
public class TxManager extends AbstractTransactionManager
    implements Lifecycle
{
    private static Logger log = Logger.getLogger( TxManager.class.getName() );

    /*
     * TODO
     * This CHM here (and the one in init()) must at some point be removed and changed
     * for something that is better bound to the transaction itself. A ThreadLocal<TransactionImpl>
     * or even better the transaction passed through the stack are improvements over this.
     * CHM will increase performance in multiple thread usage but it will reduce it in single
     * thread accesses when compared to say an ArrayMap (which was here before).
     */
    private Map<Thread, TransactionImpl> txThreadMap = new ConcurrentHashMap<Thread, TransactionImpl>();

    private final String txLogDir;
    private static String separator = File.separator;
    private String logSwitcherFileName = "active_tx_log";
    private String txLog1FileName = "tm_tx_log.1";
    private String txLog2FileName = "tm_tx_log.2";
    private final int maxTxLogRecordCount = 1000;
    private int eventIdentifierCounter = 0;

    private TxLog txLog = null;
    private boolean tmOk = false;
    private boolean blocked = false;

    private final KernelPanicEventGenerator kpe;

    private final AtomicInteger startedTxCount = new AtomicInteger( 0 );
    private final AtomicInteger comittedTxCount = new AtomicInteger( 0 );
    private final AtomicInteger rolledBackTxCount = new AtomicInteger( 0 );
    private int peakConcurrentTransactions = 0;

    private final StringLogger msgLog;

    final TxHook finishHook;
    private XaDataSourceManager xaDataSourceManager;
    private final FileSystemAbstraction fileSystem;

    public TxManager( String txLogDir,
                      XaDataSourceManager xaDataSourceManager,
                      KernelPanicEventGenerator kpe,
                      TxHook finishHook,
                      StringLogger msgLog,
                      FileSystemAbstraction fileSystem
    )
    {
        this.txLogDir = txLogDir;
        this.xaDataSourceManager = xaDataSourceManager;
        this.fileSystem = fileSystem;
        this.msgLog = msgLog;
        this.kpe = kpe;
        this.finishHook = finishHook;
    }

    synchronized int getNextEventIdentifier()
    {
        return eventIdentifierCounter++;
    }

    private <E extends Exception> E logAndReturn(String msg, E exception)
    {
        try
        {
            msgLog.logMessage(msg,exception,true);
        } catch(Throwable t)
        {
            // ignore
        }
        return exception;
    }

    @Override
    public void init()
    {
        txThreadMap = new ConcurrentHashMap<Thread, TransactionImpl>();
        logSwitcherFileName = txLogDir + separator + "active_tx_log";
        txLog1FileName = "tm_tx_log.1";
        txLog2FileName = "tm_tx_log.2";
        try
        {
            if ( fileSystem.fileExists( logSwitcherFileName ) )
            {
                FileChannel fc = fileSystem.open( logSwitcherFileName, "rw" );
                byte fileName[] = new byte[256];
                ByteBuffer buf = ByteBuffer.wrap( fileName );
                fc.read( buf );
                fc.close();
                String currentTxLog = txLogDir + separator
                    + UTF8.decode( fileName ).trim();
                if ( !fileSystem.fileExists( currentTxLog ) )
                {
                    throw logAndReturn("TM startup failure",
                            new TransactionFailureException(
                                    "Unable to start TM, " + "active tx log file[" +
                                            currentTxLog + "] not found."));
                }
                txLog = new TxLog( currentTxLog, fileSystem, msgLog );
                msgLog.logMessage( "TM opening log: " + currentTxLog, true );
            }
            else
            {
                if ( fileSystem.fileExists( txLogDir + separator + txLog1FileName )
                    || fileSystem.fileExists( txLogDir + separator + txLog2FileName ) )
                {
                    throw logAndReturn("TM startup failure",
                            new TransactionFailureException(
                                    "Unable to start TM, "
                                            + "no active tx log file found but found either "
                                            + txLog1FileName + " or " + txLog2FileName
                                            + " file, please set one of them as active or "
                                            + "remove them."));
                }
                ByteBuffer buf = ByteBuffer.wrap( txLog1FileName
                    .getBytes( "UTF-8" ) );
                FileChannel fc = fileSystem.open( logSwitcherFileName, "rw" );
                fc.write( buf );
                txLog = new TxLog( txLogDir + separator + txLog1FileName, fileSystem, msgLog );
                msgLog.logMessage( "TM new log: " + txLog1FileName, true );
                fc.force( true );
                fc.close();
            }
            tmOk = true;
        }
        catch ( IOException e )
        {
            log.log(Level.SEVERE, "Unable to start TM", e);
            throw logAndReturn("TM startup failure",
                    new TransactionFailureException("Unable to start TM", e));
        }
    }

    @Override
    public void start()
        throws Throwable
    {
        // Do recovery on start - all Resources should be registered by now
        Iterator<List<TxLog.Record>> danglingRecordList =
            txLog.getDanglingRecords();
        boolean danglingRecordFound = danglingRecordList.hasNext();
        if ( danglingRecordFound )
        {
            log.info( "Unresolved transactions found, " +
                "recovery started ..." );

            msgLog.logMessage( "TM non resolved transactions found in " + txLog.getName(), true );

            // Recover DataSources
            xaDataSourceManager.recover(danglingRecordList);

            log.info( "Recovery completed, all transactions have been " +
                "resolved to a consistent state." );
            msgLog.logMessage( "Recovery completed, all transactions have been " +
                "resolved to a consistent state." );
        }
        getTxLog().truncate();
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
        throws Throwable
    {
        if ( txLog != null )
        {
            try
            {
                txLog.close();
            }
            catch ( IOException e )
            {
                log.log( Level.WARNING, "Unable to close tx log[" + txLog.getName() + "]", e );
            }
        }
        msgLog.logMessage( "TM shutting down", true );
    }

    synchronized TxLog getTxLog() throws IOException
    {
        if ( txLog.getRecordCount() > maxTxLogRecordCount )
        {
            if ( txLog.getName().endsWith( txLog1FileName ) )
            {
                txLog.switchToLogFile( txLogDir + separator + txLog2FileName );
                changeActiveLog( txLog2FileName );
            }
            else if ( txLog.getName().endsWith( txLog2FileName ) )
            {
                txLog.switchToLogFile( txLogDir + separator + txLog1FileName );
                changeActiveLog( txLog1FileName );
            }
            else {
                setTmNotOk( new Exception( "Unknown active tx log file[" + txLog.getName()
                        + "], unable to switch." ) );
                log.severe("Unknown active tx log file[" + txLog.getName()
                        + "], unable to switch.");
                final IOException ex = new IOException("Unknown txLogFile[" + txLog.getName()
                        + "] not equals to either [" + txLog1FileName + "] or ["
                        + txLog2FileName + "]");
                throw logAndReturn("TM error accessing log file", ex);
            }
        }
        return txLog;
    }

    private void changeActiveLog( String newFileName ) throws IOException
    {
        // change active log
        FileChannel fc = fileSystem.open( logSwitcherFileName, "rw" );
        ByteBuffer buf = ByteBuffer.wrap( UTF8.encode( newFileName ) );
        fc.truncate( 0 );
        fc.write( buf );
        fc.force( true );
        fc.close();
//        msgLog.logMessage( "Active txlog set to " + newFileName, true );
    }

    void setTmNotOk( Throwable cause )
    {
        tmOk = false;
        msgLog.logMessage( "setting TM not OK", cause );
        kpe.generateEvent( ErrorState.TX_MANAGER_NOT_OK );
    }

    @Override
    public void attemptWaitForTxCompletionAndBlockFutureTransactions( long maxWaitTimeMillis )
    {
        msgLog.logMessage( "TxManager is blocking new transactions and waiting for active to fail..." );
        blocked = true;
        List<Transaction> failedTransactions = new ArrayList<Transaction>();
        synchronized ( txThreadMap )
        {
            for ( Transaction tx : txThreadMap.values() )
            {
                try
                {
                    int status = tx.getStatus();
                    if ( status != Status.STATUS_COMMITTING && status != Status.STATUS_ROLLING_BACK )
                    {   // Set it to rollback only if it's not committing or rolling back
                        tx.setRollbackOnly();
                    }
                }
                catch ( IllegalStateException e )
                {   // OK
                    failedTransactions.add( tx );
                }
                catch ( SystemException e )
                {   // OK
                    failedTransactions.add( tx );
                }
            }
        }
        msgLog.logMessage( "TxManager blocked transactions" + ((failedTransactions.isEmpty() ? "" :
                ", but failed for: " + failedTransactions.toString())) );

        long endTime = System.currentTimeMillis()+maxWaitTimeMillis;
        while ( txThreadMap.size() > 0 && System.currentTimeMillis() < endTime ) Thread.yield();
    }

    public void begin() throws NotSupportedException, SystemException
    {
        begin( ForceMode.forced );
    }

    @Override
    public void begin( ForceMode forceMode ) throws NotSupportedException, SystemException
    {
        if ( blocked )
        {
            throw new SystemException( "TxManager is preventing new transactions from starting " +
            		"due a shutdown is imminent" );
        }

        assertTmOk( "tx begin" );
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx != null )
        {
            throw logAndReturn("TM error tx begin",new NotSupportedException(
                "Nested transactions not supported" ));
        }
        tx = new TransactionImpl( this, forceMode );
        txThreadMap.put( thread, tx );
        int concurrentTxCount = txThreadMap.size();
        if ( concurrentTxCount > peakConcurrentTransactions )
        {
            peakConcurrentTransactions = concurrentTxCount;
        }
        startedTxCount.incrementAndGet();
        // start record written on resource enlistment
    }

    private void assertTmOk( String source ) throws SystemException
    {
        if ( !tmOk )
        {
            throw new SystemException( "TM has encountered some problem, "
                + "please perform neccesary action (tx recovery/restart)" );
        }
    }

    // called when a resource gets enlisted
    void writeStartRecord( byte globalId[] ) throws SystemException
    {
        try
        {
            getTxLog().txStart( globalId );
        }
        catch ( IOException e )
        {
            log.log( Level.SEVERE, "Error writing transaction log", e );
            setTmNotOk( e );
            throw logAndReturn("TM error write start record",Exceptions.withCause( new SystemException( "TM encountered a problem, "
                                                             + " error writing transaction log," ), e ));
        }
    }

    public void commit() throws RollbackException, HeuristicMixedException,
        HeuristicRollbackException, IllegalStateException, SystemException
    {
        assertTmOk( "tx commit" );
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw logAndReturn("TM error tx commit", new IllegalStateException( "Not in transaction" ));
        }

        boolean hasAnyLocks = false;
        boolean successful = false;
        try
        {
            hasAnyLocks = finishHook.hasAnyLocks( tx );
            if ( tx.getStatus() != Status.STATUS_ACTIVE
                && tx.getStatus() != Status.STATUS_MARKED_ROLLBACK )
            {
                throw logAndReturn("TM error tx commit",new IllegalStateException( "Tx status is: "
                    + getTxStatusAsString( tx.getStatus() ) ));
            }
            tx.doBeforeCompletion();
            // delist resources?
            if ( tx.getStatus() == Status.STATUS_ACTIVE )
            {
                comittedTxCount.incrementAndGet();
                commit( thread, tx );
            }
            else if ( tx.getStatus() == Status.STATUS_MARKED_ROLLBACK )
            {
                rolledBackTxCount.incrementAndGet();
                rollbackCommit( thread, tx );
            }
            else
            {
                throw logAndReturn("TM error tx commit",new IllegalStateException( "Tx status is: "
                    + getTxStatusAsString( tx.getStatus() ) ));
            }
            successful = true;
        }
        finally
        {
            if ( hasAnyLocks )
            {
                finishHook.finishTransaction( tx.getEventIdentifier(), successful );
            }
        }
    }

    private void commit( Thread thread, TransactionImpl tx )
        throws SystemException, HeuristicMixedException,
        HeuristicRollbackException
    {
        // mark as commit in log done TxImpl.doCommit()
        Throwable commitFailureCause = null;
        int xaErrorCode = -1;
        if ( tx.getResourceCount() == 0 )
        {
            tx.setStatus( Status.STATUS_COMMITTED );
        }
        else
        {
            try
            {
                tx.doCommit();
            }
            catch ( XAException e )
            {
                xaErrorCode = e.errorCode;
                log.log( Level.SEVERE, "Commit failed, status=" + getTxStatusAsString( tx.getStatus() ) +
                        ", errorCode=" + xaErrorCode, e );
                if ( tx.getStatus() == Status.STATUS_COMMITTED )
                {
                    // this should never be
                    setTmNotOk( e );
                    throw logAndReturn("TM error tx commit",new TransactionFailureException(
                        "commit threw exception but status is committed?", e ));
                }
            }
            catch ( Throwable t )
            {
                log.log( Level.SEVERE, "Commit failed", t );
                commitFailureCause = t;
            }
        }
        if ( tx.getStatus() != Status.STATUS_COMMITTED )
        {
            try
            {
                tx.doRollback();
            }
            catch ( Throwable e )
            {
                log.log( Level.SEVERE, "Unable to rollback transaction. "
                    + "Some resources may be commited others not. "
                    + "Neo4j kernel should be SHUTDOWN for "
                                       + "resource maintance and transaction recovery ---->", e );
                setTmNotOk( e );
                String commitError;
                if ( commitFailureCause != null )
                {
                    commitError = "error in commit: " + commitFailureCause;
                }
                else
                {
                    commitError = "error code in commit: " + xaErrorCode;
                }
                String rollbackErrorCode = "Uknown error code";
                if ( e instanceof XAException )
                {
                    rollbackErrorCode = Integer.toString( ( (XAException) e ).errorCode );
                }
                throw logAndReturn("TM error tx commit",Exceptions.withCause( new HeuristicMixedException( "Unable to rollback ---> " + commitError
                                        + " ---> error code for rollback: "
                                        + rollbackErrorCode ), e ) );
            }
            tx.doAfterCompletion();
            txThreadMap.remove( thread );
            try
            {
                if ( tx.isGlobalStartRecordWritten() )
                {
                    getTxLog().txDone( tx.getGlobalId() );
                }
            }
            catch ( IOException e )
            {
                log.log( Level.SEVERE, "Error writing transaction log", e );
                setTmNotOk( e );
                throw logAndReturn("TM error tx commit",Exceptions.withCause( new SystemException( "TM encountered a problem, "
                                                                 + " error writing transaction log" ), e ));
            }
            tx.setStatus( Status.STATUS_NO_TRANSACTION );
            if ( commitFailureCause == null )
            {
                throw logAndReturn("TM error tx commit",new HeuristicRollbackException(
                    "Failed to commit, transaction rolledback ---> "
                        + "error code was: " + xaErrorCode ));
            }
            else
            {
                throw logAndReturn("TM error tx commit",Exceptions.withCause( new HeuristicRollbackException(
                    "Failed to commit, transaction rolledback ---> " +
                    commitFailureCause ), commitFailureCause ));
            }
        }
        tx.doAfterCompletion();
        txThreadMap.remove( thread );
        try
        {
            if ( tx.isGlobalStartRecordWritten() )
            {
                getTxLog().txDone( tx.getGlobalId() );
            }
        }
        catch ( IOException e )
        {
            log.log( Level.SEVERE, "Error writing transaction log", e );
            setTmNotOk( e );
            throw logAndReturn("TM error tx commit",
                    Exceptions.withCause( new SystemException( "TM encountered a problem, "
                                                             + " error writing transaction log" ), e ));
        }
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
    }

    private void rollbackCommit( Thread thread, TransactionImpl tx )
        throws HeuristicMixedException, RollbackException, SystemException
    {
        try
        {
            tx.doRollback();
        }
        catch ( XAException e )
        {
            log.log( Level.SEVERE, "Unable to rollback marked transaction. "
                + "Some resources may be commited others not. "
                + "Neo4j kernel should be SHUTDOWN for "
                                   + "resource maintance and transaction recovery ---->", e );
            setTmNotOk( e );
            throw logAndReturn("TM error tx rollback commit",Exceptions.withCause(
                    new HeuristicMixedException( "Unable to rollback " + " ---> error code for rollback: "
                                                 + e.errorCode ), e ));
        }

        tx.doAfterCompletion();
        txThreadMap.remove( thread );
        try
        {
            if ( tx.isGlobalStartRecordWritten() )
            {
                getTxLog().txDone( tx.getGlobalId() );
            }
        }
        catch ( IOException e )
        {
            log.log( Level.SEVERE, "Error writing transaction log", e );
            setTmNotOk( e );
            throw logAndReturn("TM error tx rollback commit",Exceptions.withCause( new SystemException( "TM encountered a problem, "
                                                             + " error writing transaction log" ), e ));
        }
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
        throw new RollbackException(
            "Failed to commit, transaction rolledback" );
    }

    public void rollback() throws IllegalStateException, SystemException
    {
        assertTmOk( "tx rollback" );
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }

        boolean hasAnyLocks = false;
        try
        {
            hasAnyLocks = finishHook.hasAnyLocks( tx );
            if ( tx.getStatus() == Status.STATUS_ACTIVE ||
                tx.getStatus() == Status.STATUS_MARKED_ROLLBACK ||
                tx.getStatus() == Status.STATUS_PREPARING )
            {
                tx.setStatus( Status.STATUS_MARKED_ROLLBACK );
                tx.doBeforeCompletion();
                // delist resources?
                try
                {
                    rolledBackTxCount.incrementAndGet();
                    tx.doRollback();
                }
                catch ( XAException e )
                {
                    log.log( Level.SEVERE, "Unable to rollback marked or active transaction. "
                        + "Some resources may be commited others not. "
                        + "Neo4j kernel should be SHUTDOWN for "
                                           + "resource maintance and transaction recovery ---->", e );
                    setTmNotOk( e );
                    throw logAndReturn("TM error tx rollback", Exceptions.withCause(
                            new SystemException( "Unable to rollback " + " ---> error code for rollback: "
                                                 + e.errorCode ), e ));
                }
                tx.doAfterCompletion();
                txThreadMap.remove( thread );
                try
                {
                    if ( tx.isGlobalStartRecordWritten() )
                    {
                        getTxLog().txDone( tx.getGlobalId() );
                    }
                }
                catch ( IOException e )
                {
                    log.log( Level.SEVERE, "Error writing transaction log", e );
                    setTmNotOk( e );
                    throw logAndReturn("TM error tx rollback", Exceptions.withCause(
                            new SystemException( "TM encountered a problem, "
                                                 + " error writing transaction log" ), e ));
                }
                tx.setStatus( Status.STATUS_NO_TRANSACTION );
            }
            else
            {
                throw new IllegalStateException( "Tx status is: "
                    + getTxStatusAsString( tx.getStatus() ) );
            }
        }
        finally
        {
            if ( hasAnyLocks )
            {
                finishHook.finishTransaction( tx.getEventIdentifier(), false );
            }
        }
    }

    public int getStatus()
    {
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx != null )
        {
            return tx.getStatus();
        }
        return Status.STATUS_NO_TRANSACTION;
    }

    public Transaction getTransaction()
    {
        return txThreadMap.get( Thread.currentThread() );
    }

    public void resume( Transaction tx ) throws IllegalStateException,
        SystemException
    {
        assertTmOk( "tx resume" );
        Thread thread = Thread.currentThread();
        if ( txThreadMap.get( thread ) != null )
        {
            throw new IllegalStateException( "Transaction already associated" );
        }
        if ( tx != null )
        {
            TransactionImpl txImpl = (TransactionImpl) tx;
            if ( txImpl.getStatus() != Status.STATUS_NO_TRANSACTION )
            {
                if ( txImpl.isActive() )
                {
                    throw new IllegalStateException( txImpl + " already active" );
                }
                txImpl.markAsActive();
                txThreadMap.put( thread, txImpl );
            }
            // generate pro-active event resume
        }
    }

    public Transaction suspend() throws SystemException
    {
        assertTmOk( "tx suspend" );
        // check for ACTIVE/MARKED_ROLLBACK?
        TransactionImpl tx = txThreadMap.remove( Thread.currentThread() );
        if ( tx != null )
        {
            // generate pro-active event suspend
            tx.markAsSuspended();
        }
        return tx;
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        assertTmOk( "tx set rollback only" );
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        tx.setRollbackOnly();
    }

    public void setTransactionTimeout( int seconds ) throws SystemException
    {
        assertTmOk( "tx set timeout" );
        // ...
    }

    byte[] getBranchId( XAResource xaRes )
    {
        if ( xaRes instanceof XaResource )
        {
            byte branchId[] = ((XaResource) xaRes).getBranchId();
            if ( branchId != null )
            {
                return branchId;
            }
        }
        return xaDataSourceManager.getBranchId( xaRes );
    }

    String getTxStatusAsString( int status )
    {
        switch ( status )
        {
            case Status.STATUS_ACTIVE:
                return "STATUS_ACTIVE";
            case Status.STATUS_NO_TRANSACTION:
                return "STATUS_NO_TRANSACTION";
            case Status.STATUS_PREPARING:
                return "STATUS_PREPARING";
            case Status.STATUS_PREPARED:
                return "STATUS_PREPARED";
            case Status.STATUS_COMMITTING:
                return "STATUS_COMMITING";
            case Status.STATUS_COMMITTED:
                return "STATUS_COMMITED";
            case Status.STATUS_ROLLING_BACK:
                return "STATUS_ROLLING_BACK";
            case Status.STATUS_ROLLEDBACK:
                return "STATUS_ROLLEDBACK";
            case Status.STATUS_UNKNOWN:
                return "STATUS_UNKNOWN";
            case Status.STATUS_MARKED_ROLLBACK:
                return "STATUS_MARKED_ROLLBACK";
            default:
                return "STATUS_UNKNOWN(" + status + ")";
        }
    }

    public synchronized void dumpTransactions()
    {
        Iterator<TransactionImpl> itr = txThreadMap.values().iterator();
        if ( !itr.hasNext() )
        {
            System.out.println( "No uncompleted transactions" );
            return;
        }
        System.out.println( "Uncompleted transactions found: " );
        while ( itr.hasNext() )
        {
            System.out.println( itr.next() );
        }
    }

    /**
     * @return The current transaction's event identifier or -1 if no
     *         transaction is currently running.
     */
    public int getEventIdentifier()
    {
        TransactionImpl tx = (TransactionImpl) getTransaction();
        if ( tx != null )
        {
            return tx.getEventIdentifier();
        }
        return -1;
    }

    @Override
    public ForceMode getForceMode()
    {
        return ((TransactionImpl)getTransaction()).getForceMode();
    }

    public int getStartedTxCount()
    {
        return startedTxCount.get();
    }

    public int getCommittedTxCount()
    {
        return comittedTxCount.get();
    }

    public int getRolledbackTxCount()
    {
        return rolledBackTxCount.get();
    }

    public int getActiveTxCount()
    {
        return txThreadMap.size();
    }

    public int getPeakConcurrentTxCount()
    {
        return peakConcurrentTransactions;
    }
}
