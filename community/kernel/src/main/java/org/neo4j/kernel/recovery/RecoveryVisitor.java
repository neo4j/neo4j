/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.recovery;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ReentrantLockService;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.lang.Integer.max;
import static org.neo4j.kernel.impl.transaction.log.Commitment.NO_COMMITMENT;
import static org.neo4j.util.Preconditions.checkState;

class RecoveryVisitor implements RecoveryApplier
{
    private final AtomicLong prevLockedTxId = new AtomicLong( -1 );
    private final StorageEngine storageEngine;
    private final LockService lockService = new ReentrantLockService();
    private final TransactionApplicationMode mode;
    private final CursorContext cursorContext;
    private final String tracerTag;
    private final ExecutorService appliers;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final int stride;

    RecoveryVisitor( StorageEngine storageEngine, TransactionApplicationMode mode, CursorContext cursorContext, String tracerTag )
    {
        this( storageEngine, mode, cursorContext, tracerTag, max( 1, Runtime.getRuntime().availableProcessors() - 1 ) );
    }

    RecoveryVisitor( StorageEngine storageEngine, TransactionApplicationMode mode, CursorContext cursorContext, String tracerTag, int numAppliers )
    {
        this.storageEngine = storageEngine;
        this.mode = mode;
        this.cursorContext = cursorContext;
        this.tracerTag = tracerTag;
        this.appliers = new ThreadPoolExecutor( numAppliers, numAppliers, 1, TimeUnit.HOURS, new ArrayBlockingQueue<>( numAppliers ),
                new ThreadPoolExecutor.CallerRunsPolicy() );
        this.stride = mode == TransactionApplicationMode.REVERSE_RECOVERY ? -1 : 1;
    }

    @Override
    public boolean visit( CommittedTransactionRepresentation transaction ) throws Exception
    {
        checkFailure();

        // We need to know the starting point for the "is it my turn yet?" check below that each thread needs to do before acquiring the locks
        prevLockedTxId.compareAndSet( -1, transaction.getCommitEntry().getTxId() - stride );

        // TODO Also consider the memory usage of all active transaction instances and apply back-pressure if surpassing it
        appliers.submit( () ->
        {
            long txId = transaction.getCommitEntry().getTxId();
            while ( prevLockedTxId.get() != txId - stride )
            {
                Thread.onSpinWait();
                checkFailure();
            }
            try ( LockGroup locks = new LockGroup() )
            {
                storageEngine.lockRecoveryCommands( transaction.getTransactionRepresentation(), lockService, locks );
                boolean myTurn = prevLockedTxId.compareAndSet( txId - stride, txId );
                checkState( myTurn, "Something wrong with the algorithm, I thought it was my turn, but apparently it wasn't %d", txId );
                apply( transaction );
            }
            catch ( Throwable e )
            {
                failure.compareAndSet( null, e );
            }
            return null;
        } );
        return false;
    }

    private void checkFailure() throws Exception
    {
        Throwable failure = this.failure.get();
        if ( failure != null )
        {
            Exceptions.throwIfUnchecked( failure );
            throw new Exception( "One or more recovering transactions failed to apply", failure );
        }
    }

    private void apply( CommittedTransactionRepresentation transaction ) throws Exception
    {
        TransactionRepresentation txRepresentation = transaction.getTransactionRepresentation();
        long txId = transaction.getCommitEntry().getTxId();
        TransactionToApply tx = new TransactionToApply( txRepresentation, txId, cursorContext );
        tx.commitment( NO_COMMITMENT, txId );
        tx.logPosition( transaction.getStartEntry().getStartPosition() );
        storageEngine.apply( tx, mode );
    }

    @Override
    public void close() throws Exception
    {
        appliers.shutdown();
        try
        {
            if ( !appliers.awaitTermination( 1, TimeUnit.HOURS ) )
            {
                throw new IllegalStateException( "Recovery couldn't gracefully await remaining appliers" );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        checkFailure();
    }
}
