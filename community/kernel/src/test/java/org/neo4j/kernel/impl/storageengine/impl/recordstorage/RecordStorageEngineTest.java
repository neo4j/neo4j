/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.impl.EphemeralIdGenerator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordStorageEngineTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test( timeout = 30_000 )
    public void shutdownRecordStorageEngineAfterFailedTransaction() throws Throwable
    {
        RecordStorageEngine engine = start( recordStorageEngine() );
        try
        {
            Exception applicationError = executeFailingTransaction( engine );
            assertNotNull( applicationError );
        }
        finally
        {
            // shutdown should not hang after a failed transaction
            shutdown( engine );
        }
    }

    @Test
    public void databasePanicIsRaisedWhenTxApplicationFails() throws Throwable
    {
        DatabaseHealth databaseHealth = mock( DatabaseHealth.class );
        RecordStorageEngine engine = start( recordStorageEngine( databaseHealth ) );
        try
        {
            Exception applicationError = executeFailingTransaction( engine );
            verify( databaseHealth ).panic( applicationError );
        }
        finally
        {
            shutdown( engine );
        }
    }

    @Test( timeout = 30_000 )
    public void obtainCountsStoreResetterAfterFailedTransaction() throws Throwable
    {
        RecordStorageEngine engine = start( recordStorageEngine() );
        try
        {
            Exception applicationError = executeFailingTransaction( engine );
            assertNotNull( applicationError );

            CountsTracker countsStore = engine.neoStores().getCounts();
            // possible to obtain a resetting updater that internally has a write lock on the counts store
            try ( CountsAccessor.Updater updater = countsStore.reset( 0 ) )
            {
                assertNotNull( updater );
            }
        }
        finally
        {
            shutdown( engine );
        }
    }

    private static RecordStorageEngine start( RecordStorageEngine engine ) throws Throwable
    {
        engine.init();
        engine.start();
        return engine;
    }

    private static void shutdown( RecordStorageEngine engine ) throws Throwable
    {
        // this have to be done here because RecordStorageEngine creates NeoStores but does not manage their lifecycle...
        engine.neoStores().close();

        engine.stop();
        engine.shutdown();
    }

    private Exception executeFailingTransaction( RecordStorageEngine engine ) throws IOException
    {
        Exception applicationError = new UnderlyingStorageException( "No space left on device" );
        TransactionToApply txToApply = newTransactionThatFailsWith( applicationError );
        try
        {
            engine.apply( txToApply, TransactionApplicationMode.INTERNAL );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertSame( applicationError, Exceptions.rootCause( e ) );
        }
        return applicationError;
    }

    private static TransactionToApply newTransactionThatFailsWith( Exception error ) throws IOException
    {
        TransactionRepresentation transaction = mock( TransactionRepresentation.class );
        when( transaction.additionalHeader() ).thenReturn( new byte[0] );
        // allow to build validated index updates but fail on actual tx application
        doNothing().doThrow( error ).when( transaction ).accept( any() );

        long txId = ThreadLocalRandom.current().nextLong( 0, 1000 );
        TransactionToApply txToApply = new TransactionToApply( transaction );
        FakeCommitment commitment = new FakeCommitment( txId, mock( TransactionIdStore.class ) );
        commitment.setHasLegacyIndexChanges( false );
        txToApply.commitment( commitment, txId );
        return txToApply;
    }

    private RecordStorageEngine recordStorageEngine()
    {
        DatabasePanicEventGenerator panicEventGenerator = mock( DatabasePanicEventGenerator.class );
        DatabaseHealth databaseHealth = new DatabaseHealth( panicEventGenerator, NullLog.getInstance() );
        return recordStorageEngine( databaseHealth );
    }

    private RecordStorageEngine recordStorageEngine( DatabaseHealth databaseHealth )
    {
        EphemeralFileSystemAbstraction fs = fsRule.get();
        File storeDir = new File( "graph.db" );
        if ( !fs.mkdir( storeDir ) )
        {
            throw new IllegalStateException();
        }
        IdGeneratorFactory idGeneratorFactory = new EphemeralIdGenerator.Factory();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        InMemoryLabelScanStore labelScanStore = new InMemoryLabelScanStore();
        LabelScanStoreProvider labelScanStoreProvider = new LabelScanStoreProvider( labelScanStore, 42 );
        LegacyIndexProviderLookup legacyIndexProviderLookup = mock( LegacyIndexProviderLookup.class );
        when( legacyIndexProviderLookup.all() ).thenReturn( Iterables.empty() );
        IndexConfigStore indexConfigStore = new IndexConfigStore( storeDir, fs );

        return new RecordStorageEngine( storeDir, new Config(), idGeneratorFactory, pageCache, fs,
                NullLogProvider.getInstance(), mock( PropertyKeyTokenHolder.class ), mock( LabelTokenHolder.class ),
                mock( RelationshipTypeTokenHolder.class ), () -> {}, new StandardConstraintSemantics(),
                new OnDemandJobScheduler(), mock( TokenNameLookup.class ), new ReentrantLockService(),
                SchemaIndexProvider.NO_INDEX_PROVIDER, IndexingService.NO_MONITOR, databaseHealth,
                labelScanStoreProvider, legacyIndexProviderLookup, indexConfigStore );
    }
}
