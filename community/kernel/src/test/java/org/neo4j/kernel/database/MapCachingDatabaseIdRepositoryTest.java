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
package org.neo4j.kernel.database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class MapCachingDatabaseIdRepositoryTest
{
    private DatabaseIdRepository delegate = Mockito.mock( DatabaseIdRepository.class );

    private NamedDatabaseId otherNamedDbId = TestDatabaseIdRepository.randomNamedDatabaseId();
    private String otherDbName = otherNamedDbId.name();
    private DatabaseId otherDbid = otherNamedDbId.databaseId();
    private DatabaseIdRepository.Caching databaseIdRepository;

    @BeforeEach
    void setUp()
    {
        when( delegate.getByName( otherDbName ) ).thenReturn( Optional.of( otherNamedDbId ) );
        when( delegate.getById( otherDbid ) ).thenReturn( Optional.of( otherNamedDbId ) );
        databaseIdRepository = new MapCachingDatabaseIdRepository( delegate );
    }

    @Test
    void shouldDelegateGetByName()
    {
        NamedDatabaseId namedDatabaseId = databaseIdRepository.getByName( otherDbName ).get();
        assertThat( namedDatabaseId ).isEqualTo( otherNamedDbId );
    }

    @Test
    void shouldDelegateGetByUuid()
    {
        var databaseId = databaseIdRepository.getById( otherDbid ).get();
        assertThat( databaseId ).isEqualTo( otherNamedDbId );
    }

    @Test
    void shouldCacheDbByName()
    {
        databaseIdRepository.getByName( otherDbName ).get();
        databaseIdRepository.getByName( otherDbName ).get();

        verify( delegate, atMostOnce() ).getByName( otherDbName );
    }

    @Test
    void shouldCacheDbByUuid()
    {
        databaseIdRepository.getById( otherDbid ).get();
        databaseIdRepository.getById( otherDbid ).get();

        verify( delegate, atMostOnce() ).getById( otherDbid );
    }

    @Test
    void shouldInvalidateBoth()
    {
        databaseIdRepository.getByName( otherDbName ).get();
        databaseIdRepository.getById( otherDbid ).get();

        databaseIdRepository.invalidate( otherNamedDbId );

        databaseIdRepository.getByName( otherDbName ).get();
        databaseIdRepository.getById( otherDbid ).get();

        verify( delegate, times( 2 ) ).getByName( otherDbName );
        verify( delegate, times( 2 ) ).getById( otherDbid );
    }

    @Test
    void shouldCacheDbOnRequest()
    {
        databaseIdRepository.cache( otherNamedDbId );

        databaseIdRepository.getByName( otherDbName );
        databaseIdRepository.getById( otherDbid );

        verifyZeroInteractions( delegate );
    }

    @Test
    void shouldReturnSystemDatabaseIdDirectlyByName()
    {
        NamedDatabaseId namedDatabaseId = databaseIdRepository.getByName( NAMED_SYSTEM_DATABASE_ID.name() ).get();

        assertThat( namedDatabaseId ).isEqualTo( NAMED_SYSTEM_DATABASE_ID );
        verifyZeroInteractions( delegate );
    }

    @Test
    void shouldReturnSystemDatabaseIdDirectlyByUuid()
    {
        NamedDatabaseId namedDatabaseId = databaseIdRepository.getById( NAMED_SYSTEM_DATABASE_ID.databaseId() ).get();

        assertThat( namedDatabaseId ).isEqualTo( NAMED_SYSTEM_DATABASE_ID );
        verifyZeroInteractions( delegate );
    }

}
