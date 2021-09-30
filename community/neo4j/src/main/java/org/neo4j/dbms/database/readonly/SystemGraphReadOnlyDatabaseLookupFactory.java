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
package org.neo4j.dbms.database.readonly;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.SystemGraphDbmsModel;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public final class SystemGraphReadOnlyDatabaseLookupFactory implements ReadOnlyDatabases.LookupFactory
{
    private final DatabaseManager<?> databaseManager;

    public SystemGraphReadOnlyDatabaseLookupFactory( DatabaseManager<?> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    private Optional<GraphDatabaseFacade> systemDatabase()
    {
        var systemDb = databaseManager.getDatabaseContext( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID );
        var started = systemDb.map( db -> db.database().isStarted() ).orElse( false );

        if ( started )
        {
            return systemDb.map( DatabaseContext::databaseFacade );
        }
        return Optional.empty();
    }

    @Override
    public ReadOnlyDatabases.Lookup lookupReadOnlyDatabases()
    {
        return systemDatabase()
                .map( this::lookupReadOnlyDatabases )
                .map( dbs -> new SystemGraphLookup( dbs, false ) )
                .orElse( SystemGraphLookup.ALWAYS_READONLY );
    }

    private Set<NamedDatabaseId> lookupReadOnlyDatabases( GraphDatabaseFacade db )
    {
        try ( var tx = db.beginTx() )
        {
            var model = new SystemGraphDbmsModel( tx );
            var databaseAccess = model.getAllDatabaseAccess();
            return databaseAccess.entrySet().stream()
                                 .filter( e -> e.getValue() == SystemGraphDbmsModel.DatabaseAccess.READ_ONLY )
                                 .map( Map.Entry::getKey )
                                 .collect( Collectors.toUnmodifiableSet() );
        }
    }

    private static class SystemGraphLookup implements ReadOnlyDatabases.Lookup
    {
        static final SystemGraphLookup ALWAYS_READONLY = new SystemGraphLookup( Set.of(), true );

        private final Set<NamedDatabaseId> lookup;
        private final boolean alwaysReadOnly;

        SystemGraphLookup( Set<NamedDatabaseId> lookup, boolean alwaysReadOnly )
        {
            this.lookup = lookup;
            this.alwaysReadOnly = alwaysReadOnly;
        }

        @Override
        public boolean databaseIsReadOnly( NamedDatabaseId databaseId )
        {
            return alwaysReadOnly || lookup.contains( databaseId );
        }
    }
}
