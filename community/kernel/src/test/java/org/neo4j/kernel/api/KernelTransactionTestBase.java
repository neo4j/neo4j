/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.junit.Before;

import java.util.function.Supplier;

import org.neo4j.collection.pool.Pool;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KernelTransactionTestBase
{
    protected final StorageEngine storageEngine = mock( StorageEngine.class );
    protected final NeoStores neoStores = mock( NeoStores.class );
    protected final MetaDataStore metaDataStore = mock( MetaDataStore.class );
    protected final StoreReadLayer readLayer = mock( StoreReadLayer.class );
    protected final TransactionHooks hooks = new TransactionHooks();
    protected final LegacyIndexTransactionState legacyIndexState = mock( LegacyIndexTransactionState.class );
    protected final Supplier<LegacyIndexTransactionState> legacyIndexStateSupplier = () -> legacyIndexState;
    protected final TransactionMonitor transactionMonitor = mock( TransactionMonitor.class );
    protected final CapturingCommitProcess commitProcess = new CapturingCommitProcess();
    protected final TransactionHeaderInformation headerInformation = mock( TransactionHeaderInformation.class );
    protected final TransactionHeaderInformationFactory headerInformationFactory =  mock( TransactionHeaderInformationFactory.class );
    protected final SchemaWriteGuard schemaWriteGuard = mock( SchemaWriteGuard.class );
    protected final FakeClock clock = new FakeClock();
    protected final KernelTransactions kernelTransactions = mock( KernelTransactions.class );
    protected final Pool<KernelTransactionImplementation> txPool = mock( Pool.class );

    @Before
    public void before()
    {
        when( headerInformation.getAdditionalHeader() ).thenReturn( new byte[0] );
        when( headerInformationFactory.create() ).thenReturn( headerInformation );
        when( readLayer.newStatement() ).thenReturn( mock( StoreStatement.class ) );
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );
        when( storageEngine.storeReadLayer() ).thenReturn( readLayer );
    }

    public KernelTransactionImplementation newTransaction( AccessMode accessMode )
    {
        return newTransaction( 0, accessMode );
    }

    public KernelTransactionImplementation newTransaction( long lastTransactionIdWhenStarted, AccessMode accessMode )
    {
        return new KernelTransactionImplementation( null, schemaWriteGuard, hooks, null, null, headerInformationFactory,
                commitProcess, transactionMonitor, legacyIndexStateSupplier, txPool, clock, TransactionTracer.NULL,
                storageEngine ).initialize( lastTransactionIdWhenStarted, new NoOpClient(), Type.implicit,
                accessMode );
    }

    public class CapturingCommitProcess implements TransactionCommitProcess
    {
        private long txId = 1;
        public TransactionRepresentation transaction;

        @Override
        public long commit( TransactionToApply batch, CommitEvent commitEvent,
                            TransactionApplicationMode mode ) throws TransactionFailureException
        {
            assert transaction == null : "Designed to only allow one transaction";
            assert batch.next() == null : "Designed to only allow one transaction";
            transaction = batch.transactionRepresentation();
            return txId++;
        }
    }
}
