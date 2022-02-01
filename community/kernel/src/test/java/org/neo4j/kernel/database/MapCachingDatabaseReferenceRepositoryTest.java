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
package org.neo4j.kernel.database;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MapCachingDatabaseReferenceRepositoryTest
{
    private final DatabaseReferenceRepository delegate = Mockito.mock( DatabaseReferenceRepository.class );
    private final NamedDatabaseId dbId = DatabaseIdFactory.from( "random", UUID.randomUUID() );
    private final NormalizedDatabaseName name = new NormalizedDatabaseName( dbId.name() );
    private final NormalizedDatabaseName aliasName = new NormalizedDatabaseName( "foo" );
    private final DatabaseReference ref = new DatabaseReference.Internal( name, dbId );
    private final DatabaseReference aliasRef = new DatabaseReference.Internal( aliasName, dbId );

    private DatabaseReferenceRepository.Caching databaseRefRepo;

    @BeforeEach
    void setUp()
    {
        when( delegate.getByName( aliasName ) ).thenReturn( Optional.of( aliasRef ) );
        when( delegate.getByName( name ) ).thenReturn( Optional.of( ref ) );
        databaseRefRepo = new MapCachingDatabaseReferenceRepository( delegate );
    }

    @Test
    void shouldLookupByName()
    {
        var lookup = databaseRefRepo.getByName( name );
        var lookupAlias = databaseRefRepo.getByName( aliasName );
        var lookupUnknown = databaseRefRepo.getByName( new NormalizedDatabaseName( "unknown" ) );

        assertThat( lookup ).contains( ref );
        assertThat( lookupAlias ).contains( aliasRef );
        assertThat( lookupUnknown ).isEmpty();
    }

    @Test
    void shouldCacheByByName()
    {
        var lookup = databaseRefRepo.getByName( name );
        var lookup2 = databaseRefRepo.getByName( name );

        assertThat( lookup ).contains( ref );
        assertThat( lookup ).isEqualTo( lookup2 );

        verify( delegate, atMostOnce() ).getByName( name );
    }

    @Test
    void shouldNotCacheGetAllLookups()
    {
        databaseRefRepo.getAllDatabaseReferences();
        databaseRefRepo.getInternalDatabaseReferences();
        databaseRefRepo.getExternalDatabaseReferences();
        databaseRefRepo.getAllDatabaseReferences();
        databaseRefRepo.getInternalDatabaseReferences();
        databaseRefRepo.getExternalDatabaseReferences();

        verify( delegate, atLeast( 2 ) ).getAllDatabaseReferences();
        verify( delegate, atLeast( 2 ) ).getInternalDatabaseReferences();
        verify( delegate, atLeast( 2 ) ).getExternalDatabaseReferences();
    }
}
