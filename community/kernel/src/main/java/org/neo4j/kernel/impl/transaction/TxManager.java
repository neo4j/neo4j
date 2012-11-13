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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;
import org.neo4j.kernel.impl.util.ExceptionCauseSetter;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.ThreadLocalWithSize;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Default transaction manager implementation
 */
public class TxManager extends AbstractTransactionManager implements Lifecycle
{
    private ThreadLocalWithSize<TransactionImpl> txThreadMap;

    private final String txLogDir;
    private static String separator = File.separator;
    private String logSwitcherFileName = "active_tx_log";
    private String txLog1FileName = "tm_tx_log.1";
    private String txLog2FileName = "tm_tx_log.2";
    private final int maxTxLogRecordCount = 1000;
    private int eventIdentifierCounter = 0;

    private final Map<RecoveredBranchInfo, Boolean> branches = new HashMap<RecoveredBranchInfo, Boolean>();
    private volatile TxLog txLog = null;
    private boolean tmOk = false;
    private boolean blocked = false;

    private final KernelPanicEventGenerator kpe;

    private final AtomicInteger startedTxCount = new AtomicInteger( 0 );
    private final AtomicInteger comittedTxCount = new AtomicInteger( 0 );
    private final AtomicInteger rolledBackTxCount = new AtomicInteger( 0 );
    private int peakConcurrentTransactions = 0;

    private final StringLogger log;

    final TxHook finishHook;
    private XaDataSourceManager xaDataSourceManager;
    private final FileSystemAbstraction fileSystem;
    private TxManager.TxManagerDataSourceRegistrationListener dataSourceRegistrationListener;

    private Throwable recoveryError;

    private TransactionStateFactory stateFactory;

    public TxManager( String txLogDir,
                      XaDataSourceManager xaDataSourceManager,
                      KernelPanicEventGenerator kpe,
                      TxHook finishHook,
                      StringLogger log,
                      FileSystemAbstraction fileSystem,
                      TransactionStateFactory stateFactory
    )
    {
        this.txLogDir = txLogDir;
        this.xaDataSourceManager = xaDataSourceManager;
        this.fileSystem = fileSystem;
        this.log = log;
        this.kpe = kpe;
        this.finishHook = finishHook;
        this.stateFactory = stateFactory;
    }

    synchronized int getNextEventIdentifier()
    {
        return eventIdentifierCounter++;
    }

    private <E extends Exception> E logAndReturn( String msg, E exception )
    {
        try
        {
            log.logMessage( msg, exception, true );
        }
        catch ( Throwable t )
        {
            // ignore
        }
        return exception;
    }

    private volatile boolean recovered = false;

    @Override
    public void init()
    {
        txThreadMap = new ThreadLocalWithSize<TransactionImpl>();
        logSwitcherFileName = txLogDir + separator + "active_tx_log";
        txLog1FileName = "tm_tx_log.1";
        txLog2FileName = "tm_tx_log.2";
    }

    @Override
    public void start()
            throws Throwable
    {
        openLog();
        findPendingDatasources();
        dataSourceRegistrationListener = new TxManagerDataSourceRegistrationListener();
        xaDataSourceManager.addDataSourceRegistrationListener( dataSourceRegistrationListener );
    }

    private void findPendingDatasources()
    {
        try
        {
            Iterable<List<TxLog.Record>> danglingRecordList = txLog.getDanglingRecords();
            for ( List<TxLog.Record> tx : danglingRecordList )
            {
                for ( TxLog.Record rec : tx )
                {
                    if ( rec.getType() == TxLog.BRANCH_ADD )
                    {
                        RecoveredBranchInfo branchId = new RecoveredBranchInfo( rec.getBranchId()) ;
                        if ( branches.containsKey( branchId ) )
                        {
                            continue;
                        }
                        branches.put( branchId, false );
                    }
                }
            }
        }
        catch ( IOException e )
        {
            log.logMessage( "Unable to recover pending branches", e );
            throw logAndReturn( "TM startup failure",
                    new TransactionFailureException( "Unable to start TM", e ) );
        }
    }

    @Override
    public void stop()
    {
        recovered = false;
        xaDataSourceManager.removeDataSourceRegistrationListener( dataSourceRegistrationListener );
        closeLog();
    }

    @Override
    public void shutdown()
            throws Throwable
    {
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
            else
            {
                setTmNotOk( new Exception( "Unknown active tx log file[" + txLog.getName()
                        + "], unable to switch." ) );
                final IOException ex = new IOException( "Unknown txLogFile[" + txLog.getName()
                        + "] not equals to either [" + txLog1FileName + "] or ["
                        + txLog2FileName + "]" );
                throw logAndReturn( "TM error accessing log file", ex );
            }
        }
        return txLog;
    }

    private void closeLog()
    {
        if ( txLog != null )
        {
            try
            {
                txLog.close();
                txLog = null;
                recovered = false;
            }
            catch ( IOException e )
            {
                log.logMessage( "Unable to close tx log[" + txLog.getName() + "]", e );
            }
        }
        log.logMessage( "TM shutting down", true );
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
//        log.logMessage( "Active txlog set to " + newFileName, true );
    }

    void setTmNotOk( Throwable cause )
    {
        tmOk = false;
        log.logMessage( "setting TM not OK", cause );
        kpe.generateEvent( ErrorState.TX_MANAGER_NOT_OK );
    }

    @Override
    public void attemptWaitForTxCompletionAndBlockFutureTransactions( long maxWaitTimeMillis )
    {
//        log.logMessage( "TxManager is blocking new transactions and waiting for active to fail..." );
//        blocked = true;
//        List<Transaction> failedTransactions = new ArrayList<Transaction>();
//        synchronized ( txThreadMap )
//        {
//            for ( Transaction tx : txThreadMap.values() )
//            {
//                try
//                {
//                    int status = tx.getStatus();
//                    if ( status != Status.STATUS_COMMITTING && status != Status.STATUS_ROLLING_BACK )
//                    {   // Set it to rollback only if it's not committing or rolling back
//                        tx.setRollbackOnly();
//                    }
//                }
//                catch ( IllegalStateException e )
//                {   // OK
//                    failedTransactions.add( tx );
//                }
//                catch ( SystemException e )
//                {   // OK
//                    failedTransactions.add( tx );
//                }
//            }
//        }
//        log.logMessage( "TxManager blocked transactions" + ((failedTransactions.isEmpty() ? "" :
//                ", but failed for: " + failedTransactions.toString())) );
//
//        long endTime = System.currentTimeMillis() + maxWaitTimeMillis;
//        while ( txThreadMap.size() > 0 && System.currentTimeMillis() < endTime )
//        {
//            Thread.yield();
//        }
    }

    @Override
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
        TransactionImpl tx = txThreadMap.get();
        if ( tx != null )
        {
            throw logAndReturn( "TM error tx begin", new NotSupportedException(
                    "Nested transactions not supported" ) );
        }
        tx = new TransactionImpl( this, forceMode, stateFactory.create() );
        txThreadMap.set( tx );
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
            log.logMessage( "Error writing transaction log", e );
            setTmNotOk( e );
            throw logAndReturn( "TM error write start record", Exceptions.withCause( new SystemException( "TM " +
                    "encountered a problem, "
                    + " error writing transaction log," ), e ) );
        }
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException,
            HeuristicRollbackException, IllegalStateException, SystemException
    {
        assertTmOk( "tx commit" );
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get();
        if ( tx == null )
        {
            throw logAndReturn( "TM error tx commit", new IllegalStateException( "Not in transaction" ) );
        }

        boolean hasAnyLocks = false;
        boolean successful = false;
        try
        {
            hasAnyLocks = finishHook.hasAnyLocks( tx );
            if ( tx.getStatus() != Status.STATUS_ACTIVE
                    && tx.getStatus() != Status.STATUS_MARKED_ROLLBACK )
            {
                throw logAndReturn( "TM error tx commit", new IllegalStateException( "Tx status is: "
                        + getTxStatusAsString( tx.getStatus() ) ) );
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
                throw logAndReturn( "TM error tx commit", new IllegalStateException( "Tx status is: "
                        + getTxStatusAsString( tx.getStatus() ) ) );
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
                // Behold, the error handling decision maker of great power.
                //
                // The thinking behind the code below is that there are certain types of errors that we understand,
                // and know that we can safely roll back after they occur. An example would be a user trying to delete
                // a node that still has relationships. For these errors, we keep a whitelist (the switch below),
                // and roll back when they occur.
                //
                // For *all* errors that we don't know exactly what they mean, we panic and run around in circles.
                // Other errors could involve out of disk space (can't recover) or out of memory (can't recover)
                // or anything else. The point is that there is no way for us to trust the state of the system any
                // more, so we set transaction manager to not ok and expect the user to fix the problem and do recovery.
                switch(e.errorCode)
                {
                    // These are error states that we can safely recover from

                    /*
                     * User tried to delete a node that still had relationships, or in some other way violated
                     * data model constraints.
                     */
                    case XAException.XA_RBINTEGRITY:

                    /*
                     *  A network error occurred.
                     */
                    case XAException.XA_HEURCOM:
                        xaErrorCode = e.errorCode;
                        commitFailureCause = e;
                        log.logMessage( "Commit failed, status=" + getTxStatusAsString( tx.getStatus() ) +
                                ", errorCode=" + xaErrorCode, e );
                        break;

                    // Error codes where we are not *certain* that we still know the state of the system
                    default:
                        setTmNotOk( e );
                        throw logAndReturn("TM error tx commit",new TransactionFailureException(
                                "commit threw exception but status is committed?", e ));
                }
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                log.logMessage( "Commit failed", t );

                setTmNotOk( t );
                // this should never be
                throw logAndReturn("TM error tx commit",new TransactionFailureException(
                        "commit threw exception but status is committed?", t ));
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
                log.logMessage( "Unable to rollback transaction. "
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
                String rollbackErrorCode = "Unknown error code";
                if ( e instanceof XAException )
                {
                    rollbackErrorCode = Integer.toString( ((XAException) e).errorCode );
                }
                throw logAndReturn( "TM error tx commit", Exceptions.withCause( new HeuristicMixedException( "Unable " +
                        "to rollback ---> " + commitError
                        + " ---> error code for rollback: "
                        + rollbackErrorCode ), e ) );
            }
            tx.doAfterCompletion();
            txThreadMap.remove();
            try
            {
                if ( tx.isGlobalStartRecordWritten() )
                {
                    getTxLog().txDone( tx.getGlobalId() );
                }
            }
            catch ( IOException e )
            {
                log.logMessage( "Error writing transaction log", e );
                setTmNotOk( e );
                throw logAndReturn( "TM error tx commit", Exceptions.withCause( new SystemException( "TM encountered " +
                        "a problem, "
                        + " error writing transaction log" ), e ) );
            }
            tx.setStatus( Status.STATUS_NO_TRANSACTION );
            if ( commitFailureCause == null )
            {
                throw logAndReturn( "TM error tx commit", new HeuristicRollbackException(
                        "Failed to commit, transaction rolledback ---> "
                                + "error code was: " + xaErrorCode ) );
            }
            else
            {
                throw logAndReturn( "TM error tx commit", Exceptions.withCause( new HeuristicRollbackException(
                        "Failed to commit, transaction rolledback ---> " +
                                commitFailureCause ), commitFailureCause ) );
            }
        }
        tx.doAfterCompletion();
        txThreadMap.remove();
        try
        {
            if ( tx.isGlobalStartRecordWritten() )
            {
                getTxLog().txDone( tx.getGlobalId() );
            }
        }
        catch ( IOException e )
        {
            log.logMessage( "Error writing transaction log", e );
            setTmNotOk( e );
            throw logAndReturn( "TM error tx commit",
                    Exceptions.withCause( new SystemException( "TM encountered a problem, "
                            + " error writing transaction log" ), e ) );
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
            log.logMessage( "Unable to rollback marked transaction. "
                    + "Some resources may be commited others not. "
                    + "Neo4j kernel should be SHUTDOWN for "
                    + "resource maintance and transaction recovery ---->", e );
            setTmNotOk( e );
            throw logAndReturn( "TM error tx rollback commit", Exceptions.withCause(
                    new HeuristicMixedException( "Unable to rollback " + " ---> error code for rollback: "
                            + e.errorCode ), e ) );
        }

        tx.doAfterCompletion();
        txThreadMap.remove();
        try
        {
            if ( tx.isGlobalStartRecordWritten() )
            {
                getTxLog().txDone( tx.getGlobalId() );
            }
        }
        catch ( IOException e )
        {
            log.logMessage( "Error writing transaction log", e );
            setTmNotOk( e );
            throw logAndReturn( "TM error tx rollback commit", Exceptions.withCause( new SystemException( "TM " +
                    "encountered a problem, "
                    + " error writing transaction log" ), e ) );
        }
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
        RollbackException rollbackException = new RollbackException(
                "Failed to commit, transaction rolledback" );
        ExceptionCauseSetter.setCause( rollbackException, tx.getRollbackCause() );
        throw rollbackException;
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException
    {
        assertTmOk( "tx rollback" );
        TransactionImpl tx = txThreadMap.get();
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
                    log.logMessage( "Unable to rollback marked or active transaction. "
                            + "Some resources may be commited others not. "
                            + "Neo4j kernel should be SHUTDOWN for "
                            + "resource maintance and transaction recovery ---->", e );
                    setTmNotOk( e );
                    throw logAndReturn( "TM error tx rollback", Exceptions.withCause(
                            new SystemException( "Unable to rollback " + " ---> error code for rollback: "
                                    + e.errorCode ), e ) );
                }
                tx.doAfterCompletion();
                txThreadMap.remove();
                try
                {
                    if ( tx.isGlobalStartRecordWritten() )
                    {
                        getTxLog().txDone( tx.getGlobalId() );
                    }
                }
                catch ( IOException e )
                {
                    log.logMessage( "Error writing transaction log", e );
                    setTmNotOk( e );
                    throw logAndReturn( "TM error tx rollback", Exceptions.withCause(
                            new SystemException( "TM encountered a problem, "
                                    + " error writing transaction log" ), e ) );
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

    @Override
    public int getStatus()
    {
        TransactionImpl tx = txThreadMap.get();
        if ( tx != null )
        {
            return tx.getStatus();
        }
        return Status.STATUS_NO_TRANSACTION;
    }

    @Override
	public Transaction getTransaction() throws SystemException
    {
        assertTmOk( "get transaction" );
        return txThreadMap.get();
    }
    
    @Override
    public void resume( Transaction tx ) throws IllegalStateException,
            SystemException
    {
        assertTmOk( "tx resume" );
        if ( txThreadMap.get() != null )
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
                txThreadMap.set( txImpl );
            }
            // generate pro-active event resume
        }
    }

    @Override
    public Transaction suspend() throws SystemException
    {
        assertTmOk( "tx suspend" );
        // check for ACTIVE/MARKED_ROLLBACK?
        TransactionImpl tx = txThreadMap.get();
        txThreadMap.remove();
        if ( tx != null )
        {
            // generate pro-active event suspend
            tx.markAsSuspended();
        }
        return tx;
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        assertTmOk( "tx set rollback only" );
        TransactionImpl tx = txThreadMap.get();
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        tx.setRollbackOnly();
    }

    @Override
    public void setTransactionTimeout( int seconds ) throws SystemException
    {
        assertTmOk( "tx set timeout" );
        // ...
    }

    private void openLog()
    {
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
                    throw logAndReturn( "TM startup failure",
                            new TransactionFailureException(
                                    "Unable to start TM, " + "active tx log file[" +
                                            currentTxLog + "] not found." ) );
                }
                txLog = new TxLog( currentTxLog, fileSystem, log );
                log.logMessage( "TM opening log: " + currentTxLog, true );
            }
            else
            {
                if ( fileSystem.fileExists( txLogDir + separator + txLog1FileName )
                        || fileSystem.fileExists( txLogDir + separator + txLog2FileName ) )
                {
                    throw logAndReturn( "TM startup failure",
                            new TransactionFailureException(
                                    "Unable to start TM, "
                                            + "no active tx log file found but found either "
                                            + txLog1FileName + " or " + txLog2FileName
                                            + " file, please set one of them as active or "
                                            + "remove them." ) );
                }
                ByteBuffer buf = ByteBuffer.wrap( txLog1FileName
                        .getBytes( "UTF-8" ) );
                FileChannel fc = fileSystem.open( logSwitcherFileName, "rw" );
                fc.write( buf );
                txLog = new TxLog( txLogDir + separator + txLog1FileName, fileSystem, log );
                log.logMessage( "TM new log: " + txLog1FileName, true );
                fc.force( true );
                fc.close();
            }
        }
        catch ( IOException e )
        {
            log.logMessage( "Unable to start TM", e );
            throw logAndReturn( "TM startup failure",
                    new TransactionFailureException( "Unable to start TM", e ) );
        }
    }

    public void doRecovery()
    {
        if ( txLog == null )
        {
            openLog();
        }
        if ( recovered )
        {
            return;
        }
        try
        {
            // Assuming here that the last datasource to register is the Neo one
//            if ( !tmOk )
            {
                txThreadMap = new ThreadLocalWithSize<TransactionImpl>();
                // Do recovery on start - all Resources should be registered by now
                Iterable<List<TxLog.Record>> knownDanglingRecordList = txLog.getDanglingRecords();
                if ( knownDanglingRecordList.iterator().hasNext() )
                    log.info( "Unresolved transactions found, " +
                            "recovery started ... " + txLogDir );

                log.logMessage( "TM non resolved transactions found in " + txLog.getName(), true );

                // Recover DataSources
                xaDataSourceManager.recover( knownDanglingRecordList.iterator() );

                log.logMessage( "Recovery completed, all transactions have been " +
                        "resolved to a consistent state." );
                
                getTxLog().truncate();
                recovered = true;
                tmOk = true;
            }
        }
        catch ( Throwable t )
        {
            setTmNotOk( t );

            recoveryError = t;
        }
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
//        Iterator<TransactionImpl> itr = txThreadMap.values().iterator();
//        if ( !itr.hasNext() )
//        {
//            System.out.println( "No uncompleted transactions" );
//            return;
//        }
//        System.out.println( "Uncompleted transactions found: " );
//        while ( itr.hasNext() )
//        {
//            System.out.println( itr.next() );
//        }
    }

    /**
     * @return The current transaction's event identifier or -1 if no
     *         transaction is currently running.
     */
    @Override
    public int getEventIdentifier()
    {
        TransactionImpl tx = null;
        try
        {
            tx = (TransactionImpl) getTransaction();
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }

        if ( tx != null )
        {
            return tx.getEventIdentifier();
        }
        return -1;
    }

    @Override
    public ForceMode getForceMode()
    {
        try
        {
            return ((TransactionImpl)getTransaction()).getForceMode();
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public Throwable getRecoveryError()
    {
        return recoveryError;
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

    @Override
    public TransactionState getTransactionState()
    {
        Transaction tx;
        try
        {
            tx = getTransaction();
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
        return tx != null ? ((TransactionImpl)tx).getState() : TransactionState.NO_STATE;
    }
    
    private class TxManagerDataSourceRegistrationListener implements DataSourceRegistrationListener
    {
        @Override
        public void registeredDataSource( XaDataSource ds )
        {
            branches.put( new RecoveredBranchInfo( ds.getBranchId() ), true );
            boolean everythingRegistered = true;
            for ( boolean dsRegistered : branches.values() )
            {
                everythingRegistered &= dsRegistered;
            }
            if ( everythingRegistered )
            {
//                    openLog();
                doRecovery();
            }
        }

        @Override
        public void unregisteredDataSource( XaDataSource ds )
        {
            branches.put( new RecoveredBranchInfo( ds.getBranchId() ), false );
            boolean everythingUnregistered = true;
            for ( boolean dsRegistered : branches.values() )
            {
                everythingUnregistered &= !dsRegistered;
            }
            if ( everythingUnregistered )
            {
                closeLog();
            }
        }
    }

    /*
     * We use a hash map to store the branch ids. byte[] however does not offer a useful implementation of equals() or
     * hashCode(), so we need a wrapper that does that.
     */
    private static final class RecoveredBranchInfo
    {
        final byte[] branchId;

        private RecoveredBranchInfo( byte[] branchId )
        {
            this.branchId = branchId;
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( branchId );
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == null || obj.getClass() != RecoveredBranchInfo.class )
            {
                return false;
            }
            return Arrays.equals( branchId, ( ( RecoveredBranchInfo )obj ).branchId );
        }
    }
}
