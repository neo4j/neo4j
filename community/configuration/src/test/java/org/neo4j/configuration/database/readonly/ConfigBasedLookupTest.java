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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;

public class ConfigBasedLookupTest {
    private static final DatabaseId foo = DatabaseIdFactory.from(UUID.randomUUID());
    private static final DatabaseId bar = DatabaseIdFactory.from(UUID.randomUUID());
    private static final DatabaseId baz = DatabaseIdFactory.from(UUID.randomUUID());
    private final Set<DatabaseId> databases = Set.of(foo, bar, baz);
    private static final ConfigBasedLookupFactory.DatabaseIdResolver databaseIdRepository =
            Mockito.mock(ConfigBasedLookupFactory.DatabaseIdResolver.class);

    @BeforeAll
    static void setup() {
        Mockito.when(databaseIdRepository.resolve("foo")).thenReturn(Optional.of(foo));
        Mockito.when(databaseIdRepository.resolve("bar")).thenReturn(Optional.of(bar));
        Mockito.when(databaseIdRepository.resolve("baz")).thenReturn(Optional.of(baz));
    }

    @Test
    void withDefaultConfigDatabaseAreWritable() {
        var lookupFactory = new ConfigBasedLookupFactory(Config.defaults(), databaseIdRepository);
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        assertThat(databases).as("all databases should be writable").noneMatch(lookup::databaseIsReadOnly);
    }

    @Test
    void readOnlyDefaultShouldIncludeAllDatabases() {
        var config = Config.defaults(GraphDatabaseSettings.read_only_database_default, true);
        var lookupFactory = new ConfigBasedLookupFactory(config, databaseIdRepository);
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        assertThat(databases).as("all databases should be read-only").allMatch(lookup::databaseIsReadOnly);
    }

    @Test
    void readOnlyLookupShouldIncludeAllConfiguredDatabases() {
        var config = Config.defaults(GraphDatabaseSettings.read_only_databases, Set.of("foo", "bar"));
        var lookupFactory = new ConfigBasedLookupFactory(config, databaseIdRepository);
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        assertThat(lookup.databaseIsReadOnly(baz)).isFalse();
        assertThat(lookup.databaseIsReadOnly(foo)).isTrue();
        assertThat(lookup.databaseIsReadOnly(bar)).isTrue();
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
        assertThat(lookup.databaseIsReadOnly(foo)).isFalse();
        assertThat(lookup.databaseIsReadOnly(bar)).isTrue();
        assertThat(lookup.databaseIsReadOnly(baz)).isTrue();
    }
}
