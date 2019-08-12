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

    private String otherDbName = "i am a database";
    private DatabaseId otherDbId = new DatabaseId( otherDbName, UUID.randomUUID() );
    private DatabaseIdRepository.Caching databaseIdRepository;

    @BeforeEach
    void setUp()
    {
        when( delegate.get( otherDbName ) ).thenReturn( Optional.of( otherDbId ) );
        databaseIdRepository = new MapCachingDatabaseIdRepository( delegate );
    }

    @Test
    void shouldDelegateGet()
    {
        DatabaseId databaseId = databaseIdRepository.get( otherDbName ).get();

        assertThat( databaseId, equalTo( otherDbId ) );
    }

    @Test
    void shouldCacheDb()
    {
        databaseIdRepository.get( otherDbName ).get();
        databaseIdRepository.get( otherDbName ).get();

        verify( delegate, atMostOnce() ).get( otherDbName );
    }

    @Test
    void shouldInvalidateDb()
    {
        databaseIdRepository.get( otherDbName ).get();
        databaseIdRepository.invalidate( otherDbId );
        databaseIdRepository.get( otherDbName ).get();

        verify( delegate, times( 2 ) ).get( otherDbName );
    }

    @Test
    void shouldReturnSystemDatabaseIdDirectly()
    {
        DatabaseId databaseId = databaseIdRepository.get( SYSTEM_DATABASE_ID.name() ).get();

        assertThat( databaseId, equalTo( SYSTEM_DATABASE_ID ) );
        verifyZeroInteractions( delegate );
    }
}
