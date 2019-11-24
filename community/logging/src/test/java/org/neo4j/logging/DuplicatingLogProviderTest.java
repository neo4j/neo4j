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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DuplicatingLogProviderTest
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
    void shouldReturnSameLoggerForSameClass()
    {
        // Given
        DuplicatingLogProvider logProvider = new DuplicatingLogProvider();

        // Then
        DuplicatingLog log = logProvider.getLog( getClass() );
        assertThat( logProvider.getLog( DuplicatingLogProviderTest.class ) ).isSameAs( log );
    }

    @Test
    void shouldReturnSameLoggerForSameContext()
    {
        // Given
        DuplicatingLogProvider logProvider = new DuplicatingLogProvider();

        // Then
        DuplicatingLog log = logProvider.getLog( "test context" );
        assertThat( logProvider.getLog( "test context" ) ).isSameAs( log );
    }

    @Test
    void shouldRemoveLogProviderFromDuplication()
    {
        // Given
        LogProvider logProvider1 = mock( LogProvider.class );
        LogProvider logProvider2 = mock( LogProvider.class );

        doReturn( log1 ).when( logProvider1 ).getLog( any( Class.class ) );
        doReturn( log2 ).when( logProvider2 ).getLog( any( Class.class ) );

        DuplicatingLogProvider logProvider = new DuplicatingLogProvider( logProvider1, logProvider2 );

        // When
        Log log = logProvider.getLog( getClass() );
        log.info( "When the going gets weird" );
        assertTrue( logProvider.remove( logProvider1 ) );
        log.info( "The weird turn pro" );

        // Then
        verify( infoLogger1 ).log( "When the going gets weird" );
        verify( infoLogger2 ).log( "When the going gets weird" );
        verify( infoLogger2 ).log( "The weird turn pro" );
        verifyNoMoreInteractions( infoLogger1 );
        verifyNoMoreInteractions( infoLogger2 );
    }
}
