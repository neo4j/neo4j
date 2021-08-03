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
package org.neo4j.bolt.v4.runtime;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltTransaction;
import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.time.SystemNanoClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

class TransactionStateMachineV4SPITest
{
    @Test
    void shouldCheckDatabaseIdInBookmark()
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        var databaseId = from( "morty", UUID.randomUUID() );
        when( dbSpi.getNamedDatabaseId() ).thenReturn( databaseId );

        var spi = new TransactionStateMachineV4SPI( dbSpi, mock( BoltChannel.class ), mock( SystemNanoClock.class ),
                                                    mock( StatementProcessorReleaseManager.class ), "123" );

        var bookmarks = List.<Bookmark>of( new BookmarkWithDatabaseId( 42, databaseId ) );

        // When
        spi.beginTransaction( null, null, bookmarks, null, null, null, null);

        // Then
        verify( dbSpi ).beginTransaction( any(), any(), any(), eq(bookmarks), any(), any(), any(), any());
    }

    @Test
    void shouldReturnBookmarkWithPrefix()
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        var tx = mock( BoltTransaction.class );

        var databaseId = from( "morty", UUID.randomUUID() );
        when( tx.getBookmarkMetadata() ).thenReturn( new BookmarkMetadata( 42L, databaseId ));
        when( dbSpi.getNamedDatabaseId() ).thenReturn( databaseId );

        var spi = new TransactionStateMachineV4SPI( dbSpi, mock( BoltChannel.class ), mock( SystemNanoClock.class ),
                                                    mock( StatementProcessorReleaseManager.class ), "123" );

        // When
        var bookmark = spi.newestBookmark( tx );

        // Then
        verify( tx ).getBookmarkMetadata();
        assertThat( bookmark ).isInstanceOf( BookmarkWithDatabaseId.class );
        assertThat( bookmark.txId() ).isEqualTo( 42L );
    }
}
