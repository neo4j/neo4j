/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MapCachingDatabaseIdRepositoryTest {
    private final DatabaseIdRepository delegate = Mockito.mock(DatabaseIdRepository.class);

    private final NamedDatabaseId randomNamedDbId = from("randomDb", UUID.randomUUID());
    private final String randomDbName = randomNamedDbId.name();
    private final DatabaseId randomDbId = randomNamedDbId.databaseId();
    private DatabaseIdRepository.Caching databaseIdRepository;

    @BeforeEach
    void setUp() {
        when(delegate.getByName(randomDbName)).thenReturn(Optional.of(randomNamedDbId));
        when(delegate.getById(randomDbId)).thenReturn(Optional.of(randomNamedDbId));
        databaseIdRepository = new MapCachingDatabaseIdRepository(delegate);
    }

    @Test
    void shouldDelegateGetByName() {
        NamedDatabaseId namedDatabaseId =
                databaseIdRepository.getByName(randomDbName).get();
        assertThat(namedDatabaseId).isEqualTo(randomNamedDbId);
    }

    @Test
    void shouldDelegateGetByUuid() {
        var databaseId = databaseIdRepository.getById(randomDbId).get();
        assertThat(databaseId).isEqualTo(randomNamedDbId);
    }

    @Test
    void shouldCacheDbByName() {
        databaseIdRepository.getByName(randomDbName).get();
        databaseIdRepository.getByName(randomDbName).get();

        verify(delegate, atMostOnce()).getByName(randomDbName);
    }

    @Test
    void shouldCacheDbByUuid() {
        databaseIdRepository.getById(randomDbId).get();
        databaseIdRepository.getById(randomDbId).get();

        verify(delegate, atMostOnce()).getById(randomDbId);
    }

    @Test
    void shouldCacheBoth() {
        var otherNamedDbId = DatabaseIdFactory.from("otherDb", UUID.randomUUID());
        when(delegate.getByName(otherNamedDbId.name())).thenReturn(Optional.of(otherNamedDbId));
        when(delegate.getById(otherNamedDbId.databaseId())).thenReturn(Optional.of(otherNamedDbId));

        databaseIdRepository.getByName(randomDbName).get();
        databaseIdRepository.getByName(randomDbName).get();
        databaseIdRepository.getById(randomDbId).get();

        databaseIdRepository.getById(otherNamedDbId.databaseId()).get();
        databaseIdRepository.getById(otherNamedDbId.databaseId()).get();
        databaseIdRepository.getByName(otherNamedDbId.name()).get();

        verify(delegate, atMostOnce()).getByName(randomDbName);
        verify(delegate, never()).getById(randomDbId);
        verify(delegate, atMostOnce()).getById(otherNamedDbId.databaseId());
        verify(delegate, never()).getByName(otherNamedDbId.name());
    }

    @Test
    void shouldReturnSystemDatabaseIdDirectlyByName() {
        NamedDatabaseId namedDatabaseId =
                databaseIdRepository.getByName(NAMED_SYSTEM_DATABASE_ID.name()).get();

        assertThat(namedDatabaseId).isEqualTo(NAMED_SYSTEM_DATABASE_ID);
        verifyNoInteractions(delegate);
    }

    @Test
    void shouldReturnSystemDatabaseIdDirectlyByUuid() {
        NamedDatabaseId namedDatabaseId = databaseIdRepository
                .getById(NAMED_SYSTEM_DATABASE_ID.databaseId())
                .get();

        assertThat(namedDatabaseId).isEqualTo(NAMED_SYSTEM_DATABASE_ID);
        verifyNoInteractions(delegate);
    }

    @Test
    void shouldInvalidateAll() {
        var otherNamedDbId = DatabaseIdFactory.from("otherDb", UUID.randomUUID());

        databaseIdRepository.getByName(otherNamedDbId.name());
        databaseIdRepository.getById(randomDbId);

        databaseIdRepository.invalidateAll();

        databaseIdRepository.getByName(otherNamedDbId.name());
        databaseIdRepository.getById(randomDbId);

        verify(delegate, times(2)).getByName(otherNamedDbId.name());
        verify(delegate, times(2)).getById(randomDbId);
    }
}
