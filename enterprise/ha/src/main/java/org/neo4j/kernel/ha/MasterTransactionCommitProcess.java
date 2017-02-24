/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.kernel.impl.api.index.NodePropertyCommandsExtractor.mayResultInIndexUpdates;
import static org.neo4j.kernel.impl.locking.ResourceTypes.SCHEMA;
import static org.neo4j.kernel.impl.locking.ResourceTypes.schemaResource;

/**
 * Commit process on the master side in HA, where transactions either comes in from slaves committing,
 * or gets created and committed directly on the master.
 */
public class MasterTransactionCommitProcess implements TransactionCommitProcess
{
    /**
     * Detector of transactions coming in from slaves which should acquire the shared schema lock before
     * being applied (and validated for application).
     */
    private static final Visitor<StorageCommand,IOException> REQUIRES_SHARED_SCHEMA_LOCK = command ->
            command instanceof NodeCommand && mayResultInIndexUpdates( (NodeCommand) command ) ||
            command instanceof PropertyCommand && mayResultInIndexUpdates( (PropertyCommand) command );

    private final TransactionCommitProcess inner;
    private final TransactionPropagator txPropagator;
    private final IntegrityValidator validator;
    private final Monitor monitor;
    private final Locks locks;
    private final boolean reacquireSharedSchemaLockOnIncomingTransactions;

    public interface Monitor
    {
        void missedReplicas( int number );
    }

    public MasterTransactionCommitProcess( TransactionCommitProcess commitProcess, TransactionPropagator txPropagator,
            IntegrityValidator validator, Monitor monitor, Locks locks,
            boolean reacquireSharedSchemaLockOnIncomingTransactions )
    {
        this.inner = commitProcess;
        this.txPropagator = txPropagator;
        this.validator = validator;
        this.monitor = monitor;
        this.locks = locks;
        this.reacquireSharedSchemaLockOnIncomingTransactions = reacquireSharedSchemaLockOnIncomingTransactions;
    }

    @Override
    public long commit( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode )
            throws TransactionFailureException
    {
        long result;
        try ( Locks.Client locks = validate( batch ) )
        {
            result = inner.commit( batch, commitEvent, mode );
        }

        // Assuming all the transactions come from the same author
        int missedReplicas = txPropagator.committed( result, batch.transactionRepresentation().getAuthorId() );

        if ( missedReplicas > 0 )
        {
            monitor.missedReplicas( missedReplicas );
        }

        return result;
    }

    private Locks.Client validate( TransactionToApply batch ) throws TransactionFailureException
    {
        Locks.Client locks = null;
        boolean success = false;
        try
        {
            while ( batch != null )
            {
                if ( reacquireSharedSchemaLockOnIncomingTransactions )
                {
                    locks = acquireSharedSchemaLockIfTransactionResultsInIndexUpdates( batch, locks );
                }
                validator.validateTransactionStartKnowledge(
                        batch.transactionRepresentation().getLatestCommittedTxWhenStarted() );
                batch = batch.next();
            }
            success = true;
            return locks;
        }
        finally
        {
            if ( !success && locks != null )
            {
                // There was an exception which prevents us from returning the Locks.Client to the caller
                // which ultimately should have been responsible for closing it, but now we can't so
                // we need to close it ourselves in here before letting the exception propagate further.
                locks.close();
                locks = null;
            }
        }
    }

    /**
     * Looks at the transaction coming from slave and decide whether or not the shared schema lock
     * should be acquired before letting it apply.
     * <p>
     * In HA the shared schema lock isn't acquired on the master. This has been fine due to other
     * factors and guards being in place. However this was introduced when releasing the exclusive schema lock
     * during index population when creating a uniqueness constraint. This added locking guards for race
     * between constraint creating transaction (on master) and concurrent slave transactions which may
     * result in index updates for that constraint, and potentially break it.
     *
     * @param batch {@link TransactionToApply} to apply, only HEAD since linked list looping is done outside.
     * @param locks potentially existing locks client, otherwise this method will create and return
     * if there's a need to acquire a lock.
     * @return either, if {@code locks} is non-null then the same instance, or if {@code locks} is null
     * and some locking is required then a new locks instance.
     * @throws TransactionFailureException on failure to read transaction.
     */
    private Locks.Client acquireSharedSchemaLockIfTransactionResultsInIndexUpdates( TransactionToApply batch,
            Locks.Client locks ) throws TransactionFailureException
    {
        try
        {
            if ( batch.accept( REQUIRES_SHARED_SCHEMA_LOCK ) )
            {
                if ( locks == null )
                {
                    locks = this.locks.newClient();
                }
                locks.acquireShared( LockTracer.NONE, SCHEMA, schemaResource() );
            }
            return locks;
        }
        catch ( IOException e )
        {
            throw new TransactionFailureException(
                    "Weird error when trying to figure out whether or not to acquire shared schema lock", e );
        }
    }
}
