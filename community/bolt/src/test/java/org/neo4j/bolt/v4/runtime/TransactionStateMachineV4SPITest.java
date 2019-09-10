/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v4.runtime;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TransactionStateMachineV4SPITest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @Test
    void shouldCheckDatabaseIdInBookmark()
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        var databaseId = databaseIdRepository.getRaw( "molly" );
        when( dbSpi.getNamedDatabaseId() ).thenReturn( databaseId );

        var spi = new TransactionStateMachineV4SPI( dbSpi, mock( BoltChannel.class ), mock( SystemNanoClock.class ),
                mock( StatementProcessorReleaseManager.class ) );

        var bookmarks = List.<Bookmark>of( new BookmarkWithDatabaseId( 42, databaseId ) );

        // When
        spi.beginTransaction( null,  bookmarks, null, null, null );

        // Then
        verify( dbSpi ).beginTransaction( any(), any(),any(), eq(bookmarks), any(), any(), any());
    }

    @Test
    void shouldReturnBookmarkWithPrefix()
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        var tx = mock( BoltTransaction.class );

        var databaseId = databaseIdRepository.getRaw( "molly" );
        when( tx.getBookmarkMetadata() ).thenReturn( new BookmarkMetadata( 42L, databaseId ));
        when( dbSpi.getNamedDatabaseId() ).thenReturn( databaseId );

        var spi = new TransactionStateMachineV4SPI( dbSpi, mock( BoltChannel.class ), mock( SystemNanoClock.class ),
                mock( StatementProcessorReleaseManager.class ) );

        // When
        var bookmark = spi.newestBookmark( tx );

        // Then
        verify( tx ).getBookmarkMetadata();
        assertThat( bookmark, instanceOf( BookmarkWithDatabaseId.class ) );
        assertThat( bookmark.txId(), equalTo( 42L ) );
    }
}
