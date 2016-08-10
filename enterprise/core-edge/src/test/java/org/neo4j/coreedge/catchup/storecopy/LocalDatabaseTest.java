/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.storecopy;

import org.junit.Test;

import java.io.File;

import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLogProvider;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.function.Suppliers.singleton;

public class LocalDatabaseTest
{
    private TransactionIdStore offlineTxIdStore = mock( TransactionIdStore.class );
    private TransactionIdStore onlineTxIdStore = mock( TransactionIdStore.class );

    @Test
    public void shouldRetrieveStoreId() throws Throwable
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        // when
        LocalDatabase localDatabase = createLocalDatabase( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 5, 3, 4 ) );
        localDatabase.start();

        // then
        assertEquals( storeId, localDatabase.storeId() );
    }

    @Test
    public void shouldNotThrowWhenSameStoreIds() throws Throwable
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        MemberId memberId = mock( MemberId.class );
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        when( storeFetcher.storeId( memberId ) ).thenReturn( storeId );

        // when
        LocalDatabase localDatabase = createLocalDatabase( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 5, 3, 4 ) );
        localDatabase.start();

        localDatabase.ensureSameStoreId( memberId, storeFetcher );

        // no exception is thrown
    }

    @Test
    public void shouldThrowWhenDifferentStoreIds() throws Throwable
    {
        // given
        StoreId storeId = new StoreId( 6, 7, 8, 9 );
        MemberId memberId = mock( MemberId.class );
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        when( storeFetcher.storeId( memberId ) ).thenReturn( storeId );

        // when
        LocalDatabase localDatabase = createLocalDatabase( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 5, 3, 4 ) );

        try
        {
            localDatabase.ensureSameStoreId( memberId, storeFetcher );
            fail( "should have thrown " );
        }
        catch ( IllegalStateException ex )
        {
            assertThat( ex.getMessage(), containsString( "This edge machine cannot join the cluster. " +
                                                         "The local database is not empty and has a mismatching storeId:" ) );
        }
    }

    @Test
    public void storeCopyShouldResetStoreId() throws Throwable
    {
        // given
        NeoStoreDataSource mockDatasource = mock( NeoStoreDataSource.class );
        when( mockDatasource.getStoreId() ).thenReturn( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 3, 4, 5 ) );
        StoreId copied = new StoreId( 5, 6, 7, 8 );
        LocalDatabase localDatabase = createLocalDatabase( mockDatasource );
        MemberId memberId = mock( MemberId.class );
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        when( storeFetcher.storeId( memberId ) ).thenReturn( copied );

        // when
        // a local database is created with the initial store id
        localDatabase.start();

        // and stopped, the store copied, and restarted
        localDatabase.stop();
        localDatabase.copyStoreFrom( memberId, storeFetcher );
        when( mockDatasource.getStoreId() ).thenReturn( new org.neo4j.kernel.impl.store.StoreId( 5, 6, 3, 7, 8 ) );
        localDatabase.start();

        // then
        // the returned storeId should be the one of the copied store
        assertEquals( copied, localDatabase.storeId() );
    }

    @Test
    public void beingStoppedShouldReturnNonMatchingStoreId() throws Throwable
    {
        // given
        LocalDatabase localDatabase = createLocalDatabase( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 3, 4, 5 ) );

        // when
        // a local database is created with the initial store id
        localDatabase.start();

        // and stopped
        localDatabase.stop();

        // then
        // the storeId returned in this state should be the default store id
        assertEquals( StoreId.DEFAULT, localDatabase.storeId() );
    }

    /*
     * This test is effectively a combination of storeCopyShouldResetStoreId() and
     * beingStoppedShouldReturnNonMatchingStoreId(). It reproduces a problem where if the LocalDatabase was asked for
     * the storeId while copying a store (actually, any time after stop() and before copy completed) the old id would
     * kept being returned.
     * While it is technically a race, we don't need multiple threads to trigger it. Simply stopping the LocalDatabase
     * asking for the storeId and then copying/starting is enough to cover this behaviour. The reason is that
     * overlapping storeId() calls with copyStore() calls doesn't matter until the store is moved to its final
     * position, overwriting the old one. This operation can be considered atomic since it actually
     * takes effect once the datasource is started as part of start()ing the LocalDatabase.
     */
    @Test
    public void askingForStoreIdWhileStoreCopyingShouldStillLeaveNewStoreIdAfterCopyCompletes() throws Throwable
    {
        // given
        NeoStoreDataSource mockDatasource = mock( NeoStoreDataSource.class );
        when( mockDatasource.getStoreId() ).thenReturn( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 5, 3, 4 ) );
        StoreId copied = new StoreId( 5, 6, 7, 8 );
        LocalDatabase localDatabase = createLocalDatabase( mockDatasource );
        MemberId memberId = mock( MemberId.class );
        StoreFetcher storeFetcher = mock( StoreFetcher.class );
        when( storeFetcher.storeId( memberId ) ).thenReturn( copied );

        // when
        // a local database is created with the initial store id
        localDatabase.start();

        // and stopped, the store copied, and restarted
        localDatabase.stop();
        // and the storeId is asked for
        localDatabase.storeId();
        // and the store copy happens
        localDatabase.copyStoreFrom( memberId, storeFetcher );
        when( mockDatasource.getStoreId() ).thenReturn( new org.neo4j.kernel.impl.store.StoreId( 5, 6, 12, 7, 8 ) );
        localDatabase.start();

        // then
        // the returned storeId should be the one of the copied store
        assertEquals( copied, localDatabase.storeId() );
    }

    @Test
    public void shouldUseDifferentTxIdStoreSuppliersWhenStoppedAndStarted() throws Throwable
    {
        // given
        LocalDatabase localDatabase = createLocalDatabase( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 5, 3, 4 ) );

        // when
        localDatabase.isEmpty();

        // then
        verify( offlineTxIdStore, times( 1 ) ).getLastCommittedTransactionId();
        verify( onlineTxIdStore, never() ).getLastCommittedTransactionId();

        // when
        reset( offlineTxIdStore, onlineTxIdStore );
        localDatabase.start();
        localDatabase.isEmpty();

        // then
        verify( onlineTxIdStore, times( 1 ) ).getLastCommittedTransactionId();
        verify( offlineTxIdStore, never() ).getLastCommittedTransactionId();
    }

    private LocalDatabase createLocalDatabase( org.neo4j.kernel.impl.store.StoreId storeId )
    {
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        NeoStoreDataSource neoStoreDataSource = mock( NeoStoreDataSource.class );
        when( dataSourceManager.getDataSource() ).thenReturn( neoStoreDataSource );
        when( neoStoreDataSource.getStoreId() ).thenReturn( storeId );
        return new LocalDatabase( new File( "directory" ), mock( CopiedStoreRecovery.class ),
                new StoreFiles( mock( FileSystemAbstraction.class ) ), dataSourceManager,
                singleton( onlineTxIdStore ), singleton( offlineTxIdStore ),
                () -> mock( DatabaseHealth.class ), NullLogProvider.getInstance() );
    }

    private LocalDatabase createLocalDatabase( NeoStoreDataSource neoStoreDataSource )
    {
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        when( dataSourceManager.getDataSource() ).thenReturn( neoStoreDataSource );
        return new LocalDatabase( new File( "directory" ), mock( CopiedStoreRecovery.class ),
                new StoreFiles( mock( FileSystemAbstraction.class, RETURNS_MOCKS ) ), dataSourceManager,
                singleton( onlineTxIdStore ), singleton( offlineTxIdStore ),
                () -> mock( DatabaseHealth.class ), NullLogProvider.getInstance() );
    }
}
