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
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
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

    private final File txLogDir;
    private File logSwitcherFileName = null;
    private String txLog1FileName = "tm_tx_log.1";
    private String txLog2FileName = "tm_tx_log.2";
    private final int maxTxLogRecordCount = 1000;
    private final AtomicInteger eventIdentifierCounter = new AtomicInteger( 0 );

    private final Map<RecoveredBranchInfo, Boolean> branches = new HashMap<>();
    private volatile TxLog txLog = null;
    private boolean tmOk = false;
    private Throwable tmNotOkCause;

    private final KernelPanicEventGenerator kpe;

    private final AtomicInteger startedTxCount = new AtomicInteger( 0 );
    private final AtomicInteger comittedTxCount = new AtomicInteger( 0 );
    private final AtomicInteger rolledBackTxCount = new AtomicInteger( 0 );
    private int peakConcurrentTransactions = 0;

    private final StringLogger log;

    private final XaDataSourceManager xaDataSourceManager;
    private final FileSystemAbstraction fileSystem;
    private TxManager.TxManagerDataSourceRegistrationListener dataSourceRegistrationListener;

    private Throwable recoveryError;
    private final TransactionStateFactory stateFactory;

    private KernelAPI kernel;

    public TxManager( File txLogDir,
                      XaDataSourceManager xaDataSourceManager,
                      KernelPanicEventGenerator kpe,
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
        this.stateFactory = stateFactory;
    }

    int getNextEventIdentifier()
    {
        return eventIdentifierCounter.incrementAndGet();
    }

    private <E extends Exception> E logAndReturn( String msg, E exception )
    {
        try
        {
            log.logMessage( msg, exception, true );
            return exception;
        }
        catch ( Throwable t )
        {
            return exception;
        }
    }

    private volatile boolean recovered = false;

    @Override
    public void init()
    {
    }

    @Override
    public synchronized void start()
            throws Throwable
    {
        txThreadMap = new ThreadLocalWithSize<>();
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
    public synchronized void stop()
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
                txLog.switchToLogFile( new File( txLogDir, txLog2FileName ));
                changeActiveLog( txLog2FileName );
            }
            else if ( txLog.getName().endsWith( txLog2FileName ) )
            {
                txLog.switchToLogFile( new File( txLogDir, txLog1FileName ));
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

    synchronized void setTmNotOk( Throwable cause )
    {
        if ( !tmOk )
        {
            return;
        }

        tmOk = false;
        tmNotOkCause = cause;
        log.logMessage( "setting TM not OK", cause );
        kpe.generateEvent( ErrorState.TX_MANAGER_NOT_OK );
    }

    @Override
    public void begin() throws NotSupportedException, SystemException
    {
        begin( ForceMode.forced );
    }

    @Override
    public void begin( ForceMode forceMode ) throws NotSupportedException, SystemException
    {
        assertTmOk();
        TransactionImpl tx = txThreadMap.get();
        if ( tx != null )
        {
            throw logAndReturn( "TM error tx begin", new NotSupportedException(
                    "Nested transactions not supported" ) );
        }
        tx = new TransactionImpl( this, forceMode, stateFactory, log );
        txThreadMap.set( tx );
        int concurrentTxCount = txThreadMap.size();
        if ( concurrentTxCount > peakConcurrentTransactions )
        {
            peakConcurrentTransactions = concurrentTxCount;
        }
        startedTxCount.incrementAndGet();
        // start record written on resource enlistment

        tx.setKernelTransaction( kernel.newTransaction() );
    }

    private void assertTmOk() throws SystemException
    {
        if ( !tmOk )
        {
            SystemException ex = new SystemException( "TM has encountered some problem, "
                    + "please perform neccesary action (tx recovery/restart)" );
            if(tmNotOkCause != null)
            {
                ex = Exceptions.withCause( ex, tmNotOkCause );
            }

            throw ex;
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
        TransactionImpl tx = txThreadMap.get();
        if ( tx == null )
        {
            throw logAndReturn( "TM error tx commit", new IllegalStateException( "Not in transaction" ) );
        }

        boolean hasAnyLocks = false;
        boolean successful = false;
        try
        {
            assertTmOk();
            hasAnyLocks = tx.hasAnyLocks();
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
                commit( tx );
            }
            else if ( tx.getStatus() == Status.STATUS_MARKED_ROLLBACK )
            {
                rolledBackTxCount.incrementAndGet();
                rollbackCommit( tx );
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
            txThreadMap.remove();
            if ( hasAnyLocks )
            {
                if(successful)
                {
                    tx.finish( true );
                }
                else
                {
                    try
                    {
                        tx.finish( false );
                    }
                    catch(RuntimeException e)
                    {
                        log.error( "Failed to commit transaction, and was then subsequently unable to " +
                                   "finish the failed tx.", e );
                    }
                }
            }
        }
    }

    private void commit( TransactionImpl tx )
            throws SystemException, HeuristicMixedException,
            HeuristicRollbackException
    {
        // mark as commit in log done TxImpl.doCommit()
        Throwable commitFailureCause = null;
        int xaErrorCode = -1;
        synchronized (this)
        {
           /*
            * The attempt to commit and the corresponding rollback in case of failure happens under the same lock.
            * This is necessary for a transaction to be able to cleanup its state in case it fails to commit
            * without any other transaction coming in and disrupting things. Hooks will be called under this
            * lock in case of rollback but not if commit succeeds, which should be ok throughput wise. There is
            * some performance degradation related to this, since now we hold a lock over commit() for
            * (potentially) all resource managers, while without this monitor each commit() on each
            * XaResourceManager locks only that.
            */
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
                                    "commit threw exception", e ));
                    }
                }
                catch ( Throwable t )
                {
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
                            "Failed to commit, transaction rolled back ---> "
                                    + "error code was: " + xaErrorCode ) );
                }
                else
                {
                    throw logAndReturn( "TM error tx commit", Exceptions.withCause( new HeuristicRollbackException(
                            "Failed to commit, transaction rolled back ---> " +
                                    commitFailureCause ), commitFailureCause ) );
                }
            }
        }
        tx.doAfterCompletion();
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

    private void rollbackCommit( TransactionImpl tx )
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
                "Failed to commit, transaction rolled back" );
        ExceptionCauseSetter.setCause( rollbackException, tx.getRollbackCause() );
        throw rollbackException;
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException
    {
        TransactionImpl tx = txThreadMap.get();
        if ( tx == null )
        {
            throw logAndReturn( "TM error tx commit", new IllegalStateException( "Not in transaction" ) );
        }

        boolean hasAnyLocks = false;
        try
        {
            assertTmOk();
            hasAnyLocks = tx.hasAnyLocks();
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
            txThreadMap.remove();
            if ( hasAnyLocks )
            {
                tx.finish( false );
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
        assertTmOk();
        return txThreadMap.get();
    }

    @Override
    public void resume( Transaction tx ) throws IllegalStateException,
            SystemException
    {
        assertTmOk();
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
        assertTmOk();
        // check for ACTIVE/MARKED_ROLLBACK?
        TransactionImpl tx = txThreadMap.get();
        if ( tx != null )
        {
            txThreadMap.remove();

            // generate pro-active event suspend
            tx.markAsSuspended();
        }
        return tx;
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        assertTmOk();
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
        assertTmOk();
        // ...
    }

    private void openLog()
    {
        logSwitcherFileName = new File( txLogDir, "active_tx_log");
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
                File currentTxLog = new File( txLogDir, UTF8.decode( fileName ).trim());
                if ( !fileSystem.fileExists( currentTxLog ) )
                {
                    throw logAndReturn( "TM startup failure",
                            new TransactionFailureException(
                                    "Unable to start TM, " + "active tx log file[" +
                                            currentTxLog + "] not found." ) );
                }
                txLog = new TxLog( currentTxLog, fileSystem );
                log.logMessage( "TM opening log: " + currentTxLog, true );
            }
            else
            {
                if ( fileSystem.fileExists( new File( txLogDir, txLog1FileName ))
                        || fileSystem.fileExists( new File( txLogDir, txLog2FileName ) ))
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
                txLog = new TxLog( new File( txLogDir, txLog1FileName), fileSystem );
                log.info( "TM new log: " + txLog1FileName );
                fc.force( true );
                fc.close();
            }
        }
        catch ( IOException e )
        {
            log.error( "Unable to start TM", e );
            throw logAndReturn( "TM startup failure",
                    new TransactionFailureException( "Unable to start TM", e ) );
        }
    }

    @Override
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
            // Do recovery on start - all Resources should be registered by now
            Iterable<List<TxLog.Record>> knownDanglingRecordList = txLog.getDanglingRecords();
            boolean danglingRecordsFound = knownDanglingRecordList.iterator().hasNext();
            if ( danglingRecordsFound )
            {
                log.info( "Unresolved transactions found in " + txLog.getName() + ", recovery started... " );
            }

            // Recover DataSources. Always call due to some internal state using it as a trigger.
            xaDataSourceManager.recover( knownDanglingRecordList.iterator() );

            if ( danglingRecordsFound )
            {
                log.logMessage( "Recovery completed, all transactions have been " +
                        "resolved to a consistent state." );
            }

            getTxLog().truncate();
            recovered = true;
            tmOk = true;
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

    /**
     * @return The current transaction's event identifier or -1 if no
     *         transaction is currently running.
     */
    @Override
    public int getEventIdentifier()
    {
        TransactionImpl tx;
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

    @Override
    public void setKernel(KernelAPI kernel)
    {
        this.kernel = kernel;
    }

    @Override
    @SuppressWarnings("deprecation")
    public KernelTransaction getKernelTransaction()
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
        return tx != null ? ((TransactionImpl)tx).getTransactionContext() : null;
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
