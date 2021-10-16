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

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyDatabaseLookupFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

@EnterpriseDbmsExtension
public class SystemGraphReadOnlyDatabaseLookupIT
{

    @Inject
    private DatabaseManagementService dbms;
    @Inject
    private DatabaseManager<?> databaseManager;

    @Test
    void shouldCorrectlyLookupReadOnlyDatabasesInSystemGraph()
    {
        dbms.createDatabase( "foo" );
        var fooId = databaseManager.getDatabaseContext( "foo" )
                                   .map( db -> db.databaseFacade().databaseId() )
                                   .orElseThrow();
        var lookupFactory = new SystemGraphReadOnlyDatabaseLookupFactory( databaseManager, NullLogProvider.getInstance() );

        var originalLookup = lookupFactory.lookupReadOnlyDatabases();

        assertFalse( originalLookup.databaseIsReadOnly( fooId ) );

        var systemDb = dbms.database( NAMED_SYSTEM_DATABASE_ID.name() );
        systemDb.executeTransactionally( "ALTER DATABASE foo SET ACCESS READ ONLY" );

        var newLookup = lookupFactory.lookupReadOnlyDatabases();

        assertTrue( newLookup.databaseIsReadOnly( fooId ) );
    }
}
