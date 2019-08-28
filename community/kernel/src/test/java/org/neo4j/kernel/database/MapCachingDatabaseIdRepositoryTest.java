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
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

class MapCachingDatabaseIdRepositoryTest
{
    private DatabaseIdRepository delegate = Mockito.mock( DatabaseIdRepository.class );

    private DatabaseId otherDbId = TestDatabaseIdRepository.randomDatabaseId();
    private String otherDbName = otherDbId.name();
    private UUID otherUuid = otherDbId.uuid();
    private DatabaseIdRepository.Caching databaseIdRepository;

    @BeforeEach
    void setUp()
    {
        when( delegate.getByName( otherDbName ) ).thenReturn( Optional.of( otherDbId ) );
        when( delegate.getByUuid( otherUuid ) ).thenReturn( Optional.of( otherDbId ) );
        databaseIdRepository = new MapCachingDatabaseIdRepository( delegate );
    }

    @Test
    void shouldDelegateGetByName()
    {
        DatabaseId databaseId = databaseIdRepository.getByName( otherDbName ).get();
        assertThat( databaseId, equalTo( otherDbId ) );
    }

    @Test
    void shouldDelegateGetByUuid()
    {
        var databaseId = databaseIdRepository.getByUuid( otherUuid ).get();
        assertThat( databaseId, equalTo( otherDbId ) );
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
        databaseIdRepository.getByUuid( otherUuid ).get();
        databaseIdRepository.getByUuid( otherUuid ).get();

        verify( delegate, atMostOnce() ).getByUuid( otherUuid );
    }

    @Test
    void shouldInvalidateBoth()
    {
        databaseIdRepository.getByName( otherDbName ).get();
        databaseIdRepository.getByUuid( otherUuid ).get();

        databaseIdRepository.invalidate( otherDbId );

        databaseIdRepository.getByName( otherDbName ).get();
        databaseIdRepository.getByUuid( otherUuid ).get();

        verify( delegate, times( 2 ) ).getByName( otherDbName );
        verify( delegate, times( 2 ) ).getByUuid( otherUuid );
    }

    @Test
    void shouldCacheDbOnRequest()
    {
        databaseIdRepository.cache( otherDbId );

        databaseIdRepository.getByName( otherDbName );
        databaseIdRepository.getByUuid( otherUuid );

        verifyZeroInteractions( delegate );
    }

    @Test
    void shouldReturnSystemDatabaseIdDirectlyByName()
    {
        DatabaseId databaseId = databaseIdRepository.getByName( SYSTEM_DATABASE_ID.name() ).get();

        assertThat( databaseId, equalTo( SYSTEM_DATABASE_ID ) );
        verifyZeroInteractions( delegate );
    }

    @Test
    void shouldReturnSystemDatabaseIdDirectlyByUuid()
    {
        DatabaseId databaseId = databaseIdRepository.getByUuid( SYSTEM_DATABASE_ID.uuid() ).get();

        assertThat( databaseId, equalTo( SYSTEM_DATABASE_ID ) );
        verifyZeroInteractions( delegate );
    }

}
