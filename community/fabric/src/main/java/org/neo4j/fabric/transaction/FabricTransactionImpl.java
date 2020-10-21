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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.neo4j.fabric.planning.QueryType;
import org.neo4j.fabric.planning.StatementType;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;

public class FabricTransactionImpl implements FabricTransaction, CompositeTransaction, FabricTransaction.FabricExecutionContext
{
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final Set<ReadingTransaction> readingTransactions = ConcurrentHashMap.newKeySet();
    private final ReadWriteLock transactionLock = new ReentrantReadWriteLock();
    private final Lock nonExclusiveLock = transactionLock.readLock();
    private final Lock exclusiveLock = transactionLock.writeLock();
    private final FabricTransactionInfo transactionInfo;
    private final TransactionBookmarkManager bookmarkManager;
    private final ErrorReporter errorReporter;
    private final TransactionManager transactionManager;
    private final FabricConfig fabricConfig;
    private final long id;
    private final FabricRemoteExecutor.RemoteTransactionContext remoteTransactionContext;
    private final FabricLocalExecutor.LocalTransactionContext localTransactionContext;
    private final AtomicReference<StatementType> statementType = new AtomicReference<>();
    private State state = State.OPEN;
    private Status terminationStatus;
    private StatementLifecycle lastSubmittedStatement;

    private SingleDbTransaction writingTransaction;

    FabricTransactionImpl( FabricTransactionInfo transactionInfo, TransactionBookmarkManager bookmarkManager, FabricRemoteExecutor remoteExecutor,
                           FabricLocalExecutor localExecutor, ErrorReporter errorReporter, TransactionManager transactionManager,
                           FabricConfig fabricConfig )
    {
        this.transactionInfo = transactionInfo;
        this.errorReporter = errorReporter;
        this.transactionManager = transactionManager;
        this.fabricConfig = fabricConfig;
        this.bookmarkManager = bookmarkManager;
        this.id = ID_GENERATOR.incrementAndGet();

        try
        {
            remoteTransactionContext = remoteExecutor.startTransactionContext( this, transactionInfo, bookmarkManager );
            localTransactionContext = localExecutor.startTransactionContext( this, transactionInfo, bookmarkManager );
        }
        catch ( RuntimeException e )
        {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
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
                var queryAfterQuery = type.isQuery() && oldType.isQuery();
                var readQueryAfterSchema = type.isReadQuery() && oldType.isSchemaCommand();
                var schemaAfterReadQuery = type.isSchemaCommand() && oldType.isReadQuery();
                var allowedCombination = queryAfterQuery || readQueryAfterSchema || schemaAfterReadQuery;
                if ( allowedCombination )
                {
                    var writeQueryAfterReadQuery = queryAfterQuery && !type.isReadQuery() && oldType.isReadQuery();
                    var upgrade = writeQueryAfterReadQuery || schemaAfterReadQuery;
                    if ( upgrade )
                    {
                        statementType.set( type );
                    }
                }
                else
                {
                    throw new FabricException( Status.Transaction.ForbiddenDueToTransactionType, "Tried to execute %s after executing %s", type, oldType );
                }
            }
        }
    }

    public boolean isSchemaTransaction()
    {
        var type = statementType.get();
        return type != null && type.isSchemaCommand();
    }

    @Override
    public void commit()
    {
        exclusiveLock.lock();
        try
        {
            if ( state == State.TERMINATED )
            {
                // Wait for all children to be rolled back. Ignore errors
                doOnChildren( readingTransactions, writingTransaction, SingleDbTransaction::rollback );
                throw new TransactionTerminatedException( terminationStatus );
            }

            if ( state == State.CLOSED )
            {
                throw new FabricException( TransactionCommitFailed, "Trying to commit closed transaction" );
            }

            state = State.CLOSED;

            var allFailures = new ArrayList<ErrorRecord>();

            try
            {
                doOnChildren( readingTransactions, null, SingleDbTransaction::commit )
                        .forEach( error -> allFailures.add( new ErrorRecord( "Failed to commit a child read transaction", error ) ) );

                if ( !allFailures.isEmpty() )
                {
                    doOnChildren( List.of(), writingTransaction, SingleDbTransaction::rollback )
                            .forEach( error -> allFailures.add( new ErrorRecord( "Failed to rollback a child write transaction", error ) ) );
                }
                else
                {
                    doOnChildren( List.of(), writingTransaction, SingleDbTransaction::commit )
                            .forEach( error -> allFailures.add( new ErrorRecord( "Failed to commit a child write transaction", error ) ) );
                }
            }
            catch ( Exception e )
            {
                allFailures.add( new ErrorRecord( "Failed to commit composite transaction", commitFailedError() ) );
            }
            finally
            {
                remoteTransactionContext.close();
                localTransactionContext.close();
                transactionManager.removeTransaction( this );
            }

            throwIfNonEmpty( allFailures, this::commitFailedError );
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

            if ( state == State.TERMINATED )
            {
                // Wait for all children to be rolled back. Ignore errors
                doOnChildren( readingTransactions, writingTransaction, SingleDbTransaction::rollback );
                return;
            }

            if ( state == State.CLOSED )
            {
                return;
            }

            state = State.CLOSED;
            doRollback( SingleDbTransaction::rollback );
        }
        finally
        {
            exclusiveLock.unlock();
        }
    }

    private void doRollback( Function<SingleDbTransaction,Mono<Void>> operation )
    {
        var allFailures = new ArrayList<ErrorRecord>();

        try
        {
            doOnChildren( readingTransactions, writingTransaction, operation )
                    .forEach( error -> allFailures.add( new ErrorRecord( "Failed to rollback a child transaction", error ) ) );
        }
        catch ( Exception e )
        {
            allFailures.add( new ErrorRecord( "Failed to rollback composite transaction", rollbackFailedError() ) );
        }
        finally
        {
            remoteTransactionContext.close();
            localTransactionContext.close();
            transactionManager.removeTransaction( this );
        }

        throwIfNonEmpty( allFailures, this::rollbackFailedError );
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

    private void throwIfNonEmpty( List<ErrorRecord> failures, Supplier<FabricException> genericException )
    {
        if ( !failures.isEmpty() )
        {
            var exception = genericException.get();
            if ( failures.size() == 1 )
            {
                // Nothing is logged if there is just one error, because it will be logged by Bolt
                // and the log would contain two lines reporting the same thing without any additional info.
                throw Exceptions.transform( exception.status(), failures.get( 0 ).error );
            }
            else
            {
                failures.forEach( failure -> exception.addSuppressed( failure.error ) );
                failures.forEach( failure -> errorReporter.report( failure.message, failure.error, exception.status() ) );
                throw exception;
            }
        }
    }

    @Override
    public StatementResult execute( Function<FabricExecutionContext,StatementResult> runLogic )
    {
        checkTransactionOpenForStatementExecution();

        try
        {
            return runLogic.apply( this );
        }
        catch ( RuntimeException e )
        {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            rollback();
            throw Exceptions.transform( Status.Statement.ExecutionFailed, e );
        }
    }

    private void checkTransactionOpenForStatementExecution()
    {
        if ( state == State.TERMINATED )
        {
            throw new TransactionTerminatedException( terminationStatus );
        }

        if ( state == State.CLOSED )
        {
            throw new FabricException( Status.Statement.ExecutionFailed, "Trying to execute query in a closed transaction" );
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
            if ( state != State.OPEN )
            {
                return;
            }

            terminationStatus = reason;
            state = State.TERMINATED;

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
            checkTransactionOpenForStatementExecution();

            if ( writingTransaction != null )
            {
                throw multipleWriteError( location );
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
            checkTransactionOpenForStatementExecution();

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
        if ( state != State.OPEN )
        {
            return;
        }

        markForTermination( reason );
    }

    private FabricException multipleWriteError( Location attempt )
    {
        return new FabricException(
                Status.Statement.AccessMode,
                "Writing to more than one database per transaction is not allowed. Attempted write to %s, currently writing to %s",
                attempt, writingTransaction.getLocation() );
    }

    private FabricException commitFailedError()
    {
        return new FabricException(
                TransactionCommitFailed,
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

    public long getId()
    {
        return id;
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

    private enum State
    {
        OPEN,
        CLOSED,
        TERMINATED
    }

    private static class ErrorRecord
    {
        private final String message;
        private final Throwable error;

        ErrorRecord( String message, Throwable error )
        {
            this.message = message;
            this.error = error;
        }
    }
}
