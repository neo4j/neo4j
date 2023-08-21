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
package org.neo4j.configuration.database.readonly;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

public class ConfigBasedLookupTest {
    private static final NamedDatabaseId foo = DatabaseIdFactory.from("foo", UUID.randomUUID());
    private static final NamedDatabaseId bar = DatabaseIdFactory.from("bar", UUID.randomUUID());
    private static final NamedDatabaseId baz = DatabaseIdFactory.from("baz", UUID.randomUUID());
    private final Set<NamedDatabaseId> databases = Set.of(foo, bar, baz);
    private static final DatabaseIdRepository databaseIdRepository = Mockito.mock(DatabaseIdRepository.class);

    @BeforeAll
    static void setup() {
        Mockito.when(databaseIdRepository.getByName("foo")).thenReturn(Optional.of(foo));
        Mockito.when(databaseIdRepository.getByName("bar")).thenReturn(Optional.of(bar));
        Mockito.when(databaseIdRepository.getByName("baz")).thenReturn(Optional.of(baz));
    }

    @Test
    void withDefaultConfigDatabaseAreWritable() {
        var lookupFactory = new ConfigBasedLookupFactory(Config.defaults(), databaseIdRepository);
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        for (var db : databases) {
            assertFalse(lookup.databaseIsReadOnly(db.databaseId()));
        }
    }

    @Test
    void readOnlyDefaultShouldIncludeAllDatabases() {
        var config = Config.defaults(GraphDatabaseSettings.read_only_database_default, true);
        var lookupFactory = new ConfigBasedLookupFactory(config, databaseIdRepository);
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        for (var db : databases) {
            assertTrue(lookup.databaseIsReadOnly(db.databaseId()));
        }
    }

    @Test
    void readOnlyLookupShouldIncludeAllConfiguredDatabases() {
        var config = Config.defaults(GraphDatabaseSettings.read_only_databases, Set.of(foo.name(), bar.name()));
        var lookupFactory = new ConfigBasedLookupFactory(config, databaseIdRepository);
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        assertFalse(lookup.databaseIsReadOnly(baz.databaseId()));
        assertTrue(lookup.databaseIsReadOnly(foo.databaseId()));
        assertTrue(lookup.databaseIsReadOnly(bar.databaseId()));
    }

    @Test
    void readOnlyLookupShouldExcludeWritableConfiguredDatabases() {
        var config = Config.defaults(Map.of(
                GraphDatabaseSettings.read_only_database_default,
                true,
                GraphDatabaseSettings.writable_databases,
                Set.of("foo")));
        var lookupFactory = new ConfigBasedLookupFactory(config, databaseIdRepository);
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        assertFalse(lookup.databaseIsReadOnly(foo.databaseId()));
        assertTrue(lookup.databaseIsReadOnly(bar.databaseId()));
        assertTrue(lookup.databaseIsReadOnly(baz.databaseId()));
    }
}
