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
package org.neo4j.configuration.database.readonly;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigBasedLookupTest
{
    private final NamedDatabaseId foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
    private final NamedDatabaseId bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
    private final NamedDatabaseId baz = DatabaseIdFactory.from( "baz", UUID.randomUUID() );
    private final Set<NamedDatabaseId> databases = Set.of( foo, bar, baz );

    @Test
    void withDefaultConfigDatabaseAreWritable()
    {
        var lookupFactory = new ConfigBasedLookupFactory( Config.defaults() );
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        for ( var db : databases )
        {
            assertFalse( lookup.databaseIsReadOnly( db ) );
        }
    }

    @Test
    void readOnlyDefaultShouldIncludeAllDatabases()
    {
        var config = Config.defaults( GraphDatabaseSettings.read_only_database_default, true );
        var lookupFactory = new ConfigBasedLookupFactory( config );
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        for ( var db : databases )
        {
            assertTrue( lookup.databaseIsReadOnly( db ) );
        }
    }

    @Test
    void readOnlyLookupShouldIncludeAllConfiguredDatabases()
    {
        var config = Config.defaults( GraphDatabaseSettings.read_only_databases, Set.of( foo.name(), bar.name() ) );
        var lookupFactory = new ConfigBasedLookupFactory( config );
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        assertFalse( lookup.databaseIsReadOnly( baz ) );
        assertTrue( lookup.databaseIsReadOnly( foo ) );
        assertTrue( lookup.databaseIsReadOnly( bar ) );
    }

    @Test
    void readOnlyLookupShouldExcludeWritableConfiguredDatabases()
    {
        var config = Config.defaults( Map.of( GraphDatabaseSettings.read_only_database_default, true ,
                                              GraphDatabaseSettings.writable_databases, Set.of( "foo" ) ) );
        var lookupFactory = new ConfigBasedLookupFactory( config );
        var lookup = lookupFactory.lookupReadOnlyDatabases();
        assertFalse( lookup.databaseIsReadOnly( foo ) );
        assertTrue( lookup.databaseIsReadOnly( bar ) );
        assertTrue( lookup.databaseIsReadOnly( baz ) );
    }
}
