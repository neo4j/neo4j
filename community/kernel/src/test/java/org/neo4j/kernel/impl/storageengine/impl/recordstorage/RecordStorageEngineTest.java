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

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordStorageEngineTest
{
    @Rule
    public final RecordStorageEngineRule storageEngineRule = new RecordStorageEngineRule();
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test( timeout = 30_000 )
    public void shutdownRecordStorageEngineAfterFailedTransaction() throws Throwable
    {
        RecordStorageEngine engine =
                storageEngineRule.getWith( fsRule.get(), pageCacheRule.getPageCache( fsRule.get() ) ).build();
        Exception applicationError = executeFailingTransaction( engine );
        assertNotNull( applicationError );
    }

    @Test
    public void databasePanicIsRaisedWhenTxApplicationFails() throws Throwable
    {
        DatabaseHealth databaseHealth = mock( DatabaseHealth.class );
        RecordStorageEngine engine =
                storageEngineRule.getWith( fsRule.get(), pageCacheRule.getPageCache( fsRule.get() ) )
                .databaseHealth( databaseHealth )
                .build();
        Exception applicationError = executeFailingTransaction( engine );
        verify( databaseHealth ).panic( applicationError );
    }

    @Test( timeout = 30_000 )
    public void obtainCountsStoreResetterAfterFailedTransaction() throws Throwable
    {
        RecordStorageEngine engine =
                storageEngineRule.getWith( fsRule.get(), pageCacheRule.getPageCache( fsRule.get() ) ).build();
        Exception applicationError = executeFailingTransaction( engine );
        assertNotNull( applicationError );

        CountsTracker countsStore = engine.neoStores().getCounts();
        // possible to obtain a resetting updater that internally has a write lock on the counts store
        try ( CountsAccessor.Updater updater = countsStore.reset( 0 ) )
        {
            assertNotNull( updater );
        }
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
        commitment.setHasLegacyIndexChanges( false );
        txToApply.commitment( commitment, txId );
        return txToApply;
    }
}
