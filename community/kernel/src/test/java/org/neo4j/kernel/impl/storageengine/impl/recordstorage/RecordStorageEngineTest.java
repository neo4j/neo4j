/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.BatchTransactionApplierFacade;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RecordStorageEngineRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordStorageEngineTest
{
    private static final File storeDir = new File( "/storedir" );
    private final RecordStorageEngineRule storageEngineRule = new RecordStorageEngineRule();
    private final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private DatabaseHealth databaseHealth = mock( DatabaseHealth.class );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule )
            .around( pageCacheRule )
            .around( storageEngineRule );

    private static final Function<Optional<StoreType>,StoreType> assertIsPresentAndGet = optional ->
    {
        assert optional.isPresent() : "Expected optional to be present";
        return optional.get();
    };

    @Test( timeout = 30_000 )
    public void shutdownRecordStorageEngineAfterFailedTransaction() throws Throwable
    {
        RecordStorageEngine engine = buildRecordStorageEngine();
        Exception applicationError = executeFailingTransaction( engine );
        assertNotNull( applicationError );
    }

    @Test
    public void panicOnExceptionDuringCommandsApply()
    {
        IllegalStateException failure = new IllegalStateException( "Too many open files" );
        RecordStorageEngine engine = storageEngineRule
                .getWith( fsRule.get(), pageCacheRule.getPageCache( fsRule.get() ) )
                .databaseHealth( databaseHealth )
                .transactionApplierTransformer( facade -> transactionApplierFacadeTransformer( facade, failure ) )
                .build();
        CommandsToApply commandsToApply = mock( CommandsToApply.class );

        try
        {
            engine.apply( commandsToApply, TransactionApplicationMode.INTERNAL );
            fail( "Exception expected" );
        }
        catch ( Exception exception )
        {
            assertSame( failure, Exceptions.rootCause( exception ) );
        }

        verify( databaseHealth ).panic( any( Throwable.class ) );
    }

    private static BatchTransactionApplierFacade transactionApplierFacadeTransformer(
            BatchTransactionApplierFacade facade, Exception failure )
    {
        return new FailingBatchTransactionApplierFacade( failure, facade );
    }

    @Test
    public void databasePanicIsRaisedWhenTxApplicationFails() throws Throwable
    {
        RecordStorageEngine engine = buildRecordStorageEngine();
        Exception applicationError = executeFailingTransaction( engine );
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass( Exception.class );
        verify( databaseHealth ).panic( captor.capture() );
        Throwable exception = captor.getValue();
        if ( exception instanceof KernelException )
        {
            assertThat( ((KernelException) exception).status(), is( Status.General.UnknownError ) );
            exception = exception.getCause();
        }
        assertThat( exception, is( applicationError ) );
    }

    @Test( timeout = 30_000 )
    public void obtainCountsStoreResetterAfterFailedTransaction() throws Throwable
    {
        RecordStorageEngine engine = buildRecordStorageEngine();
        Exception applicationError = executeFailingTransaction( engine );
        assertNotNull( applicationError );

        CountsTracker countsStore = engine.testAccessNeoStores().getCounts();
        // possible to obtain a resetting updater that internally has a write lock on the counts store
        try ( CountsAccessor.Updater updater = countsStore.reset( 0 ) )
        {
            assertNotNull( updater );
        }
    }

    @Test
    public void mustFlushStoresWithGivenIOLimiter()
    {
        IOLimiter limiter = IOLimiter.UNLIMITED;
        FileSystemAbstraction fs = fsRule.get();
        AtomicReference<IOLimiter> observedLimiter = new AtomicReference<>();
        PageCache pageCache = new DelegatingPageCache( pageCacheRule.getPageCache( fs ) )
        {
            @Override
            public void flushAndForce( IOLimiter limiter ) throws IOException
            {
                super.flushAndForce( limiter );
                observedLimiter.set( limiter );
            }
        };

        RecordStorageEngine engine = storageEngineRule.getWith( fs, pageCache ).build();
        engine.flushAndForce( limiter );

        assertThat( observedLimiter.get(), sameInstance( limiter ) );
    }

    @Test
    public void shouldListAllStoreFiles()
    {
        RecordStorageEngine engine = buildRecordStorageEngine();

        final Collection<StoreFileMetadata> files = engine.listStorageFiles();
        Set<StoreType> expectedStoreTypes = Arrays.stream( StoreType.values() ).collect( Collectors.toSet() );

        Set<StoreType> actualStoreTypes = files.stream()
                .map( storeFileMetadata -> StoreType.typeOf( storeFileMetadata.file().getName() ) )
                .map( assertIsPresentAndGet )
                .collect( Collectors.toSet() );

        assertEquals( expectedStoreTypes, actualStoreTypes );
    }

    private RecordStorageEngine buildRecordStorageEngine()
    {
        return storageEngineRule
                .getWith( fsRule.get(), pageCacheRule.getPageCache( fsRule.get() ) )
                .storeDirectory( storeDir )
                .databaseHealth( databaseHealth )
                .build();
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
        doThrow( error ).when( transaction ).accept( any() );

        long txId = ThreadLocalRandom.current().nextLong( 0, 1000 );
        TransactionToApply txToApply = new TransactionToApply( transaction );
        FakeCommitment commitment = new FakeCommitment( txId, mock( TransactionIdStore.class ) );
        commitment.setHasExplicitIndexChanges( false );
        txToApply.commitment( commitment, txId );
        return txToApply;
    }

    private static class FailingBatchTransactionApplierFacade extends BatchTransactionApplierFacade
    {
        private Exception failure;

        FailingBatchTransactionApplierFacade( Exception failure, BatchTransactionApplier... appliers )
        {
            super( appliers );
            this.failure = failure;
        }

        @Override
        public void close() throws Exception
        {
            throw failure;
        }
    }

}
