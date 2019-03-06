/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.dmbs.database;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Logger;

import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.checkState;

public final class DefaultDatabaseManager extends AbstractDatabaseManager
{
    private final Map<String,DatabaseContext> databases = new HashMap<>();

    public DefaultDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, GlobalProcedures globalProcedures,
            Logger log, GraphDatabaseFacade graphDatabaseFacade )
    {
        super( globalModule, edition, globalProcedures, log, graphDatabaseFacade );
    }

    @Override
    public Optional<DatabaseContext> getDatabaseContext( String name )
    {
        return Optional.ofNullable( databases.get( name ) );
    }

    @Override
    public DatabaseContext createDatabase( String databaseName )
    {
        requireNonNull( databaseName );
        checkState( databases.size() < 2, "System and default database are already created. Fail to create another database:" + databaseName );
        DatabaseContext databaseContext = createNewDatabaseContext( databaseName );
        databases.put( databaseName, databaseContext );
        return databaseContext;
    }

    @Override
    public void dropDatabase( String ignore )
    {
        throw new UnsupportedOperationException( "Default database manager does not support database drop." );
    }

    @Override
    public void stopDatabase( String ignore )
    {
        throw new UnsupportedOperationException( "Default database manager does not support database stop." );
    }

    @Override
    public void startDatabase( String databaseName )
    {
        throw new UnsupportedOperationException( "Default database manager does not support starting databases." );
    }

    @Override
    public void shutdown()
    {
        databases.clear();
    }

    @Override
    protected Map<String,DatabaseContext> getDatabaseMap()
    {
        return databases;
    }
}
