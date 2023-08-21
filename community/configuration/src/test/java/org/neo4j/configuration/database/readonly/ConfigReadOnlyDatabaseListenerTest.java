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

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;

@ExtendWith(LifeExtension.class)
public class ConfigReadOnlyDatabaseListenerTest {
    @Inject
    private LifeSupport lifeSupport;

    @Test
    void configChangeShouldRefreshReadOnlyDatabases() {
        // given
        var config = Config.defaults();
        var readOnlyDatabases = mock(ReadOnlyDatabases.class);
        var listener = new ConfigReadOnlyDatabaseListener(readOnlyDatabases, config);
        lifeSupport.add(listener);

        // when
        config.setDynamic(GraphDatabaseSettings.read_only_database_default, true, "test");
        config.setDynamic(GraphDatabaseSettings.writable_databases, Set.of("foo", "bar"), "test");
        config.setDynamic(GraphDatabaseSettings.read_only_databases, Set.of("baz"), "test");

        // then
        Mockito.verify(readOnlyDatabases, atLeast(3)).refresh();
    }
}
