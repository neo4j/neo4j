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
package org.neo4j.dbms.database;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.logging.NullLogProvider;

class DatabaseLifecyclesTest {
    private final Database system = mock(Database.class);
    private final Database neo4j = mock(Database.class);
    private final DatabaseRepository<StandaloneDatabaseContext> databaseRepository =
            new DatabaseRepository<>(new SimpleDatabaseIdRepository());
    private final DatabaseLifecycles databaseLifecycles = new DatabaseLifecycles(
            databaseRepository,
            DEFAULT_DATABASE_NAME,
            (namedDatabaseId, databaseOptions) -> getContext(namedDatabaseId),
            NullLogProvider.getInstance());

    @Test
    void shouldCreateSystemOmInitThenStart() throws Exception {
        // when
        var lifecycle = databaseLifecycles.systemDatabaseStarter();
        lifecycle.init();

        // then
        assertThat(databaseRepository.getDatabaseContext(DatabaseId.SYSTEM_DATABASE_ID))
                .isPresent();
        verify(system, never()).start();

        lifecycle.start();

        // then
        verify(system).start();
    }

    @Test
    void shutdownsSystemDbLast() throws Exception {
        // given
        var systemDatabaseStarter = databaseLifecycles.systemDatabaseStarter();
        systemDatabaseStarter.init();
        systemDatabaseStarter.start();
        databaseLifecycles.defaultDatabaseStarter().start();

        // when
        databaseLifecycles.allDatabaseShutdown().stop();

        // then
        InOrder inOrder = inOrder(system, neo4j);

        inOrder.verify(neo4j).stop();
        inOrder.verify(system).stop();
    }

    @Test
    void shouldCreateAndStartDefault() throws Exception {
        databaseLifecycles.defaultDatabaseStarter().start();
        verify(neo4j).start();
        assertThat(databaseRepository.getDatabaseContext(DEFAULT_DATABASE_NAME)).isPresent();
    }

    private StandaloneDatabaseContext getContext(NamedDatabaseId namedDatabaseId) {
        var mock = mock(StandaloneDatabaseContext.class);
        if (namedDatabaseId.name().equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)) {
            when(mock.database()).thenReturn(system);
        } else if (namedDatabaseId.name().equals(DEFAULT_DATABASE_NAME)) {
            when(mock.database()).thenReturn(neo4j);
        } else {
            throw new IllegalArgumentException("Not expected id " + namedDatabaseId);
        }
        return mock;
    }

    private static class SimpleDatabaseIdRepository implements DatabaseIdRepository {
        private final NamedDatabaseId defaultId = DatabaseIdFactory.from(DEFAULT_DATABASE_NAME, UUID.randomUUID());

        @Override
        public Optional<NamedDatabaseId> getByName(NormalizedDatabaseName databaseName) {
            return Optional.of(defaultId);
        }

        @Override
        public Optional<NamedDatabaseId> getById(DatabaseId databaseId) {
            return Optional.of(defaultId);
        }

        @Override
        public Map<NormalizedDatabaseName, NamedDatabaseId> getAllDatabaseAliases() {
            return null;
        }

        @Override
        public Set<NamedDatabaseId> getAllDatabaseIds() {
            return null;
        }
    }
}
