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
package org.neo4j.dbms.database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedDatabaseName;

class DatabaseRepositoryTest {
    private final SimpleIdRepository idRepository = new SimpleIdRepository();
    private final DatabaseRepository<DatabaseContext> databaseRepository = new DatabaseRepository<>(idRepository);

    @Test
    void returnsDatabasesInCorrectOrder() {
        // given
        List<NamedDatabaseId> expectedNames = List.of(
                NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID,
                from("custom", UUID.randomUUID()),
                from(DEFAULT_DATABASE_NAME, UUID.randomUUID()));
        expectedNames.forEach(dbId -> databaseRepository.add(dbId, mock(DatabaseContext.class)));

        // when
        List<NamedDatabaseId> actualNames = databaseRepository.registeredDatabases().navigableKeySet().stream()
                .toList();

        // then
        assertEquals(expectedNames, actualNames);
    }

    @Test
    void shouldAddDatabaseAndRemoveDatabase() {
        // given
        var dbId = from("db", UUID.randomUUID());
        idRepository.add(dbId);

        // when
        databaseRepository.add(dbId, mock(DatabaseContext.class));

        // then
        assertThat(databaseRepository.getDatabaseContext("db")).isPresent();
        assertThat(databaseRepository.getDatabaseContext(dbId)).isPresent();
        assertThat(databaseRepository.getDatabaseContext(dbId.databaseId())).isPresent();

        // and
        databaseRepository.remove(dbId);

        // then
        assertThat(databaseRepository.getDatabaseContext("db")).isEmpty();
        assertThat(databaseRepository.getDatabaseContext(dbId)).isEmpty();
        assertThat(databaseRepository.getDatabaseContext(dbId.databaseId())).isEmpty();
    }

    @Test
    void shouldNotAddToIdRepository() {
        // given
        var dbId = from("db", UUID.randomUUID());

        // when
        databaseRepository.add(dbId, mock(DatabaseContext.class));

        // then
        // id repository is independent, usually lives in system-db while database repository is an in memory map of
        // installed databases.
        assertThat(databaseRepository.databaseIdRepository().getById(dbId.databaseId()))
                .isEmpty();
    }

    private static class SimpleIdRepository implements DatabaseIdRepository {
        private final Map<NormalizedDatabaseName, NamedDatabaseId> databaseIds = new HashMap<>();

        void add(NamedDatabaseId dbId) {
            databaseIds.put(new NormalizedDatabaseName(dbId.name()), dbId);
        }

        @Override
        public Optional<NamedDatabaseId> getByName(NormalizedDatabaseName databaseName) {
            return Optional.ofNullable(databaseIds.get(databaseName));
        }

        @Override
        public Optional<NamedDatabaseId> getById(DatabaseId databaseId) {
            return databaseIds.values().stream()
                    .filter(id -> id.databaseId().equals(databaseId))
                    .findFirst();
        }

        @Override
        public Optional<NamedDatabaseId> getByName(String databaseName) {
            return getByName(new NormalizedDatabaseName(databaseName));
        }
    }
}
