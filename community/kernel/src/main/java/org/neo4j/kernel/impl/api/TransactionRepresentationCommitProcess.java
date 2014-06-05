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
package org.neo4j.kernel.impl.api;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.PropertyLoader;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoTransactionIndexApplier;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoTransactionStoreApplier;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionStore;

public class TransactionRepresentationCommitProcess
{
    private final TransactionStore transactionStore;
    private final KernelHealth kernelHealth;
    private final IndexingService indexingService;
    private final NeoStore neoStore;
    private final boolean recovery;
    private final LabelScanStore labelScanStore;
    private final CacheAccessBackDoor cacheAccess;
    private final LockService lockService;
    private final PropertyLoader propertyLoader;

    public TransactionRepresentationCommitProcess( TransactionStore transactionStore,
            KernelHealth kernelHealth, IndexingService indexingService, LabelScanStore labelScanStore, NeoStore neoStore,
            CacheAccessBackDoor cacheAccess, LockService lockService, boolean recovery )
    {
        this.transactionStore = transactionStore;
        this.labelScanStore = labelScanStore;
        this.neoStore = neoStore;
        this.cacheAccess = cacheAccess;
        this.lockService = lockService;
        this.recovery = recovery;
        this.kernelHealth = kernelHealth;
        this.indexingService = indexingService;
        this.propertyLoader = new PropertyLoader( neoStore );
    }

    public void commit( TransactionRepresentation representation ) throws TransactionFailureException
    {
        // write it to the log
        Future<Long> commitFuture;
        try
        {
            commitFuture = transactionStore.getAppender().append( representation );
        }
        catch ( IOException e )
        {
            kernelHealth.panic( e );
            throw new TransactionFailureException( Status.Transaction.CouldNotWriteToLog, e,
                    "Could not write transaction representation to log" );
        }

        // wait for the transaction to be written to the log
        long transactionId;
        try
        {
            transactionId = commitFuture.get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new TransactionFailureException( Status.Transaction.CouldNotWriteToLog, e, "" );
        }

        try
        {
            // apply changes to the store
            NeoTransactionStoreApplier storeApplier = new NeoTransactionStoreApplier(
                    neoStore, indexingService, cacheAccess, lockService, transactionId, recovery );
            try
            {
                representation.accept( storeApplier );
            }
            finally
            {
                storeApplier.done();
            }

            // apply changes to the schema indexes
            NeoTransactionIndexApplier indexApplier = new NeoTransactionIndexApplier( indexingService,
                    labelScanStore, neoStore.getNodeStore(), neoStore.getPropertyStore(), cacheAccess, propertyLoader );
            try
            {
                representation.accept( indexApplier );
            }
            finally
            {
                indexApplier.done();
            }

            neoStore.transactionClosed( transactionId );
            // TODO 2.2-future remove updateIdGenerators and setRecovered as a whole
            if ( recovery )
            {
                neoStore.updateIdGenerators();
            }
        }
        // TODO catch different types of exceptions here, some which are OK
        catch ( IOException e )
        {
            kernelHealth.panic( e );
            throw new TransactionFailureException( Status.Transaction.CouldNotCommit, e,
                    "Could not apply the transaction to the store after written to log" );
        }
    }
}
