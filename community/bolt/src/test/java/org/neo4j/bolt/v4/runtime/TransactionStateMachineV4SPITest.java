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

import java.time.Duration;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmarkMixture;

class TransactionStateMachineV4SPITest
{
    @Test
    void shouldCheckDatabaseIdInBookmark() throws Throwable
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        var databaseId = mock( DatabaseId.class );
        when( databaseId.name() ).thenReturn( "molly" );
        when( dbSpi.getDatabaseId() ).thenReturn( databaseId );

        var txDuration = Duration.ofMinutes( 10 );
        var spi = new TransactionStateMachineV4SPI( dbSpi, mock( BoltChannel.class ), txDuration, mock( SystemNanoClock.class ),
                mock( StatementProcessorReleaseManager.class ) );

        var bookmark = mock( Bookmark.class );
        when( bookmark.txId() ).thenReturn( 42L );
        when( bookmark.databaseId() ).thenReturn( "molly" );

        // When
        spi.awaitUpToDate( bookmark );

        // Then
        verify( bookmark ).txId();
        verify( bookmark ).databaseId();
        verify( dbSpi ).awaitUpToDate( eq( 42L ), eq( txDuration ) );
    }

    @Test
    void shouldErrorIfDatabaseIdMismatch() throws Throwable
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        var databaseId = mock( DatabaseId.class );
        when( databaseId.name() ).thenReturn( "molly" );
        when( dbSpi.getDatabaseId() ).thenReturn( databaseId );

        var txDuration = Duration.ofMinutes( 10 );
        var spi = new TransactionStateMachineV4SPI( dbSpi, mock( BoltChannel.class ), txDuration, mock( SystemNanoClock.class ),
                mock( StatementProcessorReleaseManager.class ) );

        var bookmark = mock( Bookmark.class );
        when( bookmark.txId() ).thenReturn( 42L );
        when( bookmark.databaseId() ).thenReturn( "bossie" );

        // When
        var e = assertThrows( TransactionFailureException.class, () -> spi.awaitUpToDate( bookmark ) );

        // Then
        assertThat( e.status(), equalTo( InvalidBookmarkMixture ) );
        verify( bookmark, never() ).txId();
        verify( bookmark ).databaseId();
        verify( dbSpi, never() ).awaitUpToDate( eq( 42L ), eq( txDuration ) );
    }

    @Test
    void shouldReturnBookmarkWithPrefix() throws Throwable
    {
        // Given
        var dbSpi = mock( BoltGraphDatabaseServiceSPI.class );
        when( dbSpi.newestEncounteredTxId() ).thenReturn( 42L );
        var databaseId = mock( DatabaseId.class );
        when( databaseId.name() ).thenReturn( "molly" );
        when( dbSpi.getDatabaseId() ).thenReturn( databaseId );

        var txDuration = Duration.ofMinutes( 10 );
        var spi = new TransactionStateMachineV4SPI( dbSpi, mock( BoltChannel.class ), txDuration, mock( SystemNanoClock.class ),
                mock( StatementProcessorReleaseManager.class ) );

        // When
        var bookmark = spi.newestBookmark();

        // Then
        verify( dbSpi ).newestEncounteredTxId();
        assertThat( bookmark, instanceOf( BookmarkWithDatabaseId.class ) );
        assertThat( bookmark.txId(), equalTo( 42L ) );
    }
}
