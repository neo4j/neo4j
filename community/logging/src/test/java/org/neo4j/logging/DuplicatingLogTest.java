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
package org.neo4j.logging;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DuplicatingLogTest
{
    final Log log1 = mock( Log.class );
    final Log log2 = mock( Log.class );
    final Logger infoLogger1 = mock( Logger.class );
    final Logger infoLogger2 = mock( Logger.class );

    @BeforeEach
    void beforeEach()
    {
        when( log1.infoLogger() ).thenReturn( infoLogger1 );
        when( log2.infoLogger() ).thenReturn( infoLogger2 );
    }

    @Test
    void shouldOutputToMultipleLogs()
    {
        // Given
        DuplicatingLog log = new DuplicatingLog( log1, log2 );

        // When
        log.info( "When the going gets weird" );

        // Then
        verify( infoLogger1 ).log( "When the going gets weird" );
        verify( infoLogger2 ).log( "When the going gets weird" );
        verifyNoMoreInteractions( infoLogger1 );
        verifyNoMoreInteractions( infoLogger2 );
    }

    @Test
    void shouldBulkOutputToMultipleLogs()
    {
        final Answer bulkAnswer = invocationOnMock ->
        {
            final Consumer<Log> consumer = invocationOnMock.getArgument( 0 );
            consumer.accept( (Log) invocationOnMock.getMock() );
            return null;
        };

        doAnswer( bulkAnswer ).when( log1 ).bulk( any() );
        doAnswer( bulkAnswer ).when( log2 ).bulk( any() );

        // Given
        DuplicatingLog log = new DuplicatingLog( log1, log2 );

        // When
        log.bulk( bulkLog -> bulkLog.info( "When the going gets weird" ) );

        // Then
        verify( infoLogger1 ).log( "When the going gets weird" );
        verify( infoLogger2 ).log( "When the going gets weird" );

        verifyNoMoreInteractions( infoLogger1 );
        verifyNoMoreInteractions( infoLogger2 );
    }

    @Test
    void shouldRemoveLogFromDuplication()
    {
        // Given
        DuplicatingLog log = new DuplicatingLog( log1, log2 );

        // When
        log.info( "When the going gets weird" );
        log.remove( log1 );
        log.info( "The weird turn pro" );

        // Then
        verify( infoLogger1 ).log( "When the going gets weird" );
        verify( infoLogger2 ).log( "When the going gets weird" );
        verify( infoLogger2 ).log( "The weird turn pro" );
        verifyNoMoreInteractions( infoLogger1 );
        verifyNoMoreInteractions( infoLogger2 );
    }

    @Test
    void shouldRemoveLoggersFromDuplication()
    {
        // Given
        DuplicatingLog log = new DuplicatingLog( log1, log2 );
        Logger logger = log.infoLogger();

        // When
        logger.log( "When the going gets weird" );
        log.remove( log1 );
        logger.log( "The weird turn pro" );

        // Then
        verify( infoLogger1 ).log( "When the going gets weird" );
        verify( infoLogger2 ).log( "When the going gets weird" );
        verify( infoLogger2 ).log( "The weird turn pro" );
        verifyNoMoreInteractions( infoLogger1 );
        verifyNoMoreInteractions( infoLogger2 );
    }
}
