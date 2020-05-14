/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.fabric.transaction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.Exceptions;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.FabricStatementLifecycles.StatementLifecycle;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.SingleDbTransaction;
import org.neo4j.fabric.planning.StatementType;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.scheduler.Group.SERVER_TRANSACTION_TIMEOUT;

public class FabricTransactionImpl implements FabricTransaction, CompositeTransaction, FabricTransaction.FabricExecutionContext
{
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final Set<ReadingTransaction> readingTransactions = ConcurrentHashMap.newKeySet();
    private final ReadWriteLock transactionLock = new ReentrantReadWriteLock();
    private final Lock nonExclusiveLock = transactionLock.readLock();
    private final Lock exclusiveLock = transactionLock.writeLock();
    private final FabricTransactionInfo transactionInfo;
    private final TransactionBookmarkManager bookmarkManager;
    private final Log userLog;
    private final Log internalLog;
    private final TransactionManager transactionManager;
    private final JobScheduler jobScheduler;
    private final FabricConfig fabricConfig;
    private final long id;
    private final FabricRemoteExecutor.RemoteTransactionContext remoteTransactionContext;
    private final FabricLocalExecutor.LocalTransactionContext localTransactionContext;
    private JobHandle timeoutHandle;
    private boolean terminated;
    private Status terminationStatus;
    private AtomicReference<StatementType> statementType = new AtomicReference<>();
    private StatementLifecycle lastSubmittedStatement;

    private SingleDbTransaction writingTransaction;

    FabricTransactionImpl( FabricTransactionInfo transactionInfo, TransactionBookmarkManager bookmarkManager, FabricRemoteExecutor remoteExecutor,
                           FabricLocalExecutor localExecutor, LogService logService, TransactionManager transactionManager, JobScheduler jobScheduler,
                           FabricConfig fabricConfig )
    {
        this.transactionInfo = transactionInfo;
        this.userLog = logService.getUserLog( FabricTransactionImpl.class );
        this.internalLog = logService.getInternalLog( FabricTransactionImpl.class );
        this.transactionManager = transactionManager;
        this.jobScheduler = jobScheduler;
        this.fabricConfig = fabricConfig;
        this.bookmarkManager = bookmarkManager;
        this.id = ID_GENERATOR.incrementAndGet();

        internalLog.debug( "Starting transaction %d", id );

        try
        {
            remoteTransactionContext = remoteExecutor.startTransactionContext( this, transactionInfo, bookmarkManager );
            localTransactionContext = localExecutor.startTransactionContext( this, transactionInfo, bookmarkManager );

            scheduleTimeout( transactionInfo );
            internalLog.debug( "Transaction %d started", id );
        }
        catch ( RuntimeException e )
        {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            userLog.error( "Transaction {} start failed", id );
            throw Exceptions.transform( Status.Transaction.TransactionStartFailed, e );
        }
    }

    @Override
    public FabricTransactionInfo getTransactionInfo()
    {
        return transactionInfo;
    }

    @Override
    public FabricRemoteExecutor.RemoteTransactionContext getRemote()
    {
        return remoteTransactionContext;
    }

    @Override
    public FabricLocalExecutor.LocalTransactionContext getLocal()
    {
        return localTransactionContext;
    }

    @Override
    public void validateStatementType( StatementType type )
    {
        boolean wasNull = statementType.compareAndSet( null, type );
        if ( !wasNull )
        {
            var oldType = statementType.get();
            if ( oldType != type )
            {
                throw new FabricException( Status.Transaction.ForbiddenDueToTransactionType, "Tried to execute %s after executing %s", type, oldType );
            }
        }
    }

    @Override
    public void commit()
    {
        exclusiveLock.lock();
        try
        {
            // the transaction has failed and been rolled back as part of the failure clean up
            if ( terminated )
            {
                // Wait for all children to be rolled back. Ignore errors
                doOnChildren( readingTransactions, writingTransaction, SingleDbTransaction::rollback );

                var reason = getReasonIfTerminated().map( s -> s.code().description() ).orElse( "Trying to commit terminated transaction" );
                throw new FabricException( Status.Transaction.TransactionCommitFailed, reason );
            }
            terminated = true;

            internalLog.debug( "Committing transaction %d", id );

            var allFailures = new ArrayList<Throwable>();

            try
            {
                cancelTimeout();

                allFailures.addAll( doOnChildren( readingTransactions, null, SingleDbTransaction::commit ) );
                allFailures.forEach( err -> userLog.error( "Failed to commit a child read transaction", err ) );

                List<Throwable> errors;
                if ( !allFailures.isEmpty() )
                {
                    errors = doOnChildren( List.of(), writingTransaction, SingleDbTransaction::rollback );
                    errors.forEach( err -> userLog.error( "Failed to rollback a child write transaction", err ) );
                }
                else
                {
                    errors = doOnChildren( List.of(), writingTransaction, SingleDbTransaction::commit );
                    errors.forEach( err -> userLog.error( "Failed to commit a child write transaction", err ) );
                }
                allFailures.addAll( errors );
            }
            catch ( Exception e )
            {
                allFailures.add( commitFailedError() );
            }
            finally
            {
                remoteTransactionContext.close();
                localTransactionContext.close();
                transactionManager.removeTransaction( this );
            }

            throwIfNonEmpty( allFailures, this::commitFailedError );

            internalLog.debug( "Transaction %d committed", id );
        }
        finally
        {
            exclusiveLock.unlock();
        }
    }

    @Override
    public void rollback()
    {
        exclusiveLock.lock();
        try
        {
            // guard against someone calling rollback after 'begin' failure
            if ( remoteTransactionContext == null && localTransactionContext == null )
            {
                return;
            }

            if ( terminated )
            {
                // Wait for all children to be rolled back. Ignore errors
                doOnChildren( readingTransactions, writingTransaction, SingleDbTransaction::rollback );
                return;
            }

            doRollback( SingleDbTransaction::rollback );
        }
        finally
        {
            exclusiveLock.unlock();
        }
    }

    private void doRollback( Function<SingleDbTransaction,Mono<Void>> operation )
    {
        terminated = true;
        internalLog.debug( "Rolling back transaction %d", id );

        var allFailures = new ArrayList<Throwable>();

        try
        {
            cancelTimeout();

            allFailures.addAll( doOnChildren( readingTransactions, writingTransaction, operation ) );
            allFailures.forEach( err -> userLog.error( "Failed to rollback a child read transaction", err ) );
        }
        catch ( Exception e )
        {
            allFailures.add( rollbackFailedError() );
        }
        finally
        {
            remoteTransactionContext.close();
            localTransactionContext.close();
            transactionManager.removeTransaction( this );
        }

        throwIfNonEmpty( allFailures, this::rollbackFailedError );

        internalLog.debug( "Transaction %d rolled back", id );
    }

    private List<Throwable> doOnChildren( Iterable<ReadingTransaction> readingTransactions,
                                          SingleDbTransaction writingTransaction,
                                          Function<SingleDbTransaction,Mono<Void>> operation )
    {
        var failures = Flux
                .fromIterable( readingTransactions )
                .map( txWrapper -> txWrapper.singleDbTransaction )
                .concatWith( Mono.justOrEmpty( writingTransaction ) )
                .flatMap( tx -> catchErrors( operation.apply( tx ) ) )
                .collectList()
                .block();

        return failures == null ? List.of() : failures;
    }

    private Mono<Throwable> catchErrors( Mono<Void> action )
    {
        return action
                .flatMap( v -> Mono.<Throwable>empty() )
                .onErrorResume( Mono::just );
    }

    private void throwIfNonEmpty( List<Throwable> failures, Supplier<FabricException> genericException )
    {
        if ( !failures.isEmpty() )
        {
            var exception = genericException.get();
            if ( failures.size() == 1 )
            {
                throw Exceptions.transform( exception.status(), failures.get( 0 ) );
            }
            else
            {
                failures.forEach( exception::addSuppressed );
                throw exception;
            }
        }
    }

    @Override
    public StatementResult execute( Function<FabricExecutionContext,StatementResult> runLogic )
    {
        if ( terminated )
        {
            Status status = terminationStatus;
            if ( status == null )
            {
                status = Status.Statement.ExecutionFailed;
            }

            internalLog.error( "Trying to execute query in a terminated transaction %d", id );
            throw new FabricException( status, "Trying to execute query in a terminated transaction" );
        }

        try
        {
            return runLogic.apply( this );
        }
        catch ( RuntimeException e )
        {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            userLog.error( "Query execution in transaction %d failed", id );
            rollback();
            throw Exceptions.transform( Status.Statement.ExecutionFailed, e );
        }
    }

    @Override
    public void setLastSubmittedStatement( StatementLifecycle statement )
    {
        lastSubmittedStatement = statement;
    }

    @Override
    public Optional<StatementLifecycle> getLastSubmittedStatement()
    {
        return Optional.ofNullable( lastSubmittedStatement );
    }

    @Override
    public void markForTermination( Status reason )
    {
        exclusiveLock.lock();
        try
        {
            if ( terminated )
            {
                return;
            }

            internalLog.debug( "Terminating transaction %d", id );
            terminationStatus = reason;

            doRollback( singleDbTransaction -> singleDbTransaction.terminate( reason ) );
        }
        finally
        {
            exclusiveLock.unlock();
        }
    }

    @Override
    public Optional<Status> getReasonIfTerminated()
    {
        if ( terminationStatus != null )
        {
            return Optional.of( terminationStatus );
        }

        return Optional.empty();
    }

    @Override
    public TransactionBookmarkManager getBookmarkManager()
    {
        return bookmarkManager;
    }

    @Override
    public void setMetaData( Map<String,Object> txMeta )
    {
        transactionInfo.setMetaData( txMeta );
        for ( InternalTransaction internalTransaction : getInternalTransactions() )
        {
            internalTransaction.setMetaData( txMeta );
        }
    }

    @Override
    public <TX extends SingleDbTransaction> TX startWritingTransaction( Location location, Supplier<TX> writeTransactionSupplier ) throws FabricException
    {
        exclusiveLock.lock();
        try
        {
            if ( terminated )
            {
                throw parentTransactionTerminatedError( location );
            }

            if ( writingTransaction != null )
            {
                throw multipleWriteError( writingTransaction.getLocation() );
            }

            var tx = writeTransactionSupplier.get();
            writingTransaction = tx;
            return tx;
        }
        finally
        {
            exclusiveLock.unlock();
        }
    }

    @Override
    public <TX extends SingleDbTransaction> TX startReadingTransaction( Location location, Supplier<TX> readingTransactionSupplier ) throws FabricException
    {
        return startReadingTransaction( location, false, readingTransactionSupplier );
    }

    @Override
    public <TX extends SingleDbTransaction> TX startReadingOnlyTransaction( Location location, Supplier<TX> readingTransactionSupplier ) throws FabricException
    {
        return startReadingTransaction( location, true, readingTransactionSupplier );
    }

    private <TX extends SingleDbTransaction> TX startReadingTransaction( Location location, boolean readOnly, Supplier<TX> readingTransactionSupplier )
            throws FabricException
    {
        nonExclusiveLock.lock();
        try
        {
            if ( terminated )
            {
                throw parentTransactionTerminatedError( location );
            }

            var tx = readingTransactionSupplier.get();
            readingTransactions.add( new ReadingTransaction( tx, readOnly ) );
            return tx;
        }
        finally
        {
            nonExclusiveLock.unlock();
        }
    }

    @Override
    public <TX extends SingleDbTransaction> void upgradeToWritingTransaction( TX writingTransaction ) throws FabricException
    {
        if ( this.writingTransaction == writingTransaction )
        {
            return;
        }

        exclusiveLock.lock();
        try
        {
            if ( this.writingTransaction == writingTransaction )
            {
                return;
            }

            if ( this.writingTransaction != null )
            {
                throw multipleWriteError( writingTransaction.getLocation() );
            }

            ReadingTransaction readingTransaction = readingTransactions
                    .stream()
                    .filter( readingTx -> readingTx.singleDbTransaction == writingTransaction )
                    .findAny()
                    .orElseThrow( () -> new IllegalArgumentException(
                            "The supplied transaction has not been registered" ) );

            if ( readingTransaction.readingOnly )
            {
                throw new IllegalStateException( "Upgrading reading-only transaction to a writing one is not allowed" );
            }

            readingTransactions.remove( readingTransaction );
            this.writingTransaction = readingTransaction.singleDbTransaction;
        }
        finally
        {
            exclusiveLock.unlock();
        }
    }

    @Override
    public void childTransactionTerminated( Status reason )
    {
        if ( terminated )
        {
            return;
        }

        internalLog.debug( "Child transaction belonging to composite transaction %d terminated", id );
        markForTermination( reason );
    }

    private FabricException multipleWriteError( Location attempt )
    {
        return new FabricException(
                Status.Fabric.AccessMode,
                "Multi-shard writes not allowed. Attempted write to %s, currently writing to %s",
                attempt, writingTransaction.getLocation() );
    }

    private FabricException parentTransactionTerminatedError( Location location )
    {
        return new FabricException(
                Status.Transaction.TransactionStartFailed,
                "Could not start a transaction at %s, because the parent composite transaction has terminated",
                location );
    }

    private FabricException commitFailedError()
    {
        return new FabricException(
                Status.Transaction.TransactionCommitFailed,
                "Failed to commit composite transaction %d",
                id );
    }

    private FabricException rollbackFailedError()
    {
        return new FabricException(
                Status.Transaction.TransactionRollbackFailed,
                "Failed to rollback composite transaction %d",
                id );
    }

    private void scheduleTimeout( FabricTransactionInfo transactionInfo )
    {
        if ( transactionInfo.getTxTimeout() != null )
        {
            scheduleTimeout( transactionInfo.getTxTimeout() );
            return;
        }

        scheduleTimeout( fabricConfig.getTransactionTimeout() );
    }

    private void scheduleTimeout( Duration duration )
    {
        // 0 means no timeout
        if ( duration.equals( Duration.ZERO ) )
        {
            return;
        }

        timeoutHandle = jobScheduler.schedule( SERVER_TRANSACTION_TIMEOUT, this::handleTimeout, duration.toSeconds(), TimeUnit.SECONDS );
    }

    private void handleTimeout()
    {
        exclusiveLock.lock();
        try
        {
            // the transaction has already been rolled back as part of the failure clean up
            if ( terminated )
            {
                return;
            }

            userLog.info( "Terminating transaction %d because of timeout", id );
            terminationStatus = Status.Transaction.TransactionTimedOut;
            doRollback( singleDbTransaction -> singleDbTransaction.terminate( terminationStatus ) );
        }
        finally
        {
            exclusiveLock.unlock();
        }
    }

    private void cancelTimeout()
    {
        if ( timeoutHandle != null )
        {
            timeoutHandle.cancel();
        }
    }

    private static class ReadingTransaction
    {
        private final SingleDbTransaction singleDbTransaction;
        private final boolean readingOnly;

        ReadingTransaction( SingleDbTransaction singleDbTransaction, boolean readingOnly )
        {
            this.singleDbTransaction = singleDbTransaction;
            this.readingOnly = readingOnly;
        }
    }

    public Set<InternalTransaction> getInternalTransactions()
    {
        return localTransactionContext.getInternalTransactions();
    }
}
