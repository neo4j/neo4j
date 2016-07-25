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

import org.neo4j.coreedge.catchup.storecopy.edge.CopiedStoreRecovery;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreFetcher;
import org.neo4j.coreedge.server.MemberId;
import org.neo4j.coreedge.server.StoreId;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.function.Suppliers.singleton;

public class LocalDatabaseTest
{
    @Test
    public void shouldRetrieveStoreId() throws Throwable
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        // when
        LocalDatabase localDatabase = createLocalDatabase( new org.neo4j.kernel.impl.store.StoreId( 1, 2, 5, 3, 4 )  );

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
            fail( "should have thrown ");
        }
        catch ( IllegalStateException ex )
        {
            assertThat( ex.getMessage(), containsString( "This edge machine cannot join the cluster. " +
                    "The local database is not empty and has a mismatching storeId:" ) );
        }
    }

    private LocalDatabase createLocalDatabase( org.neo4j.kernel.impl.store.StoreId storeId )
    {
        DataSourceManager dataSourceManager = mock( DataSourceManager.class );
        NeoStoreDataSource neoStoreDataSource = mock( NeoStoreDataSource.class );
        when( dataSourceManager.getDataSource() ).thenReturn( neoStoreDataSource );
        when( neoStoreDataSource.getStoreId() ).thenReturn( storeId );
        return new LocalDatabase( new File( "directory" ), mock( CopiedStoreRecovery.class ),
                new StoreFiles( mock( FileSystemAbstraction.class ) ), dataSourceManager,
                singleton( mock( TransactionIdStore.class ) ), () -> mock( DatabaseHealth.class ), NullLogProvider.getInstance() );
    }
}
