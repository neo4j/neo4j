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
package org.neo4j.dbms.database;

import java.util.Optional;

import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;

import static java.util.Objects.requireNonNull;

public final class DefaultDatabaseManager extends AbstractDatabaseManager<StandaloneDatabaseContext>
{

    public DefaultDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log, GraphDatabaseFacade graphDatabaseFacade )
    {
        super( globalModule, edition, log, graphDatabaseFacade );
    }

    @Override
    public Optional<StandaloneDatabaseContext> getDatabaseContext( DatabaseId databaseId )
    {
        return Optional.ofNullable( databaseMap.get( databaseId ) );
    }

    @Override
    public synchronized StandaloneDatabaseContext createDatabase( DatabaseId databaseId )
    {
        requireNonNull( databaseId );
        checkDatabaseLimit( databaseId );
        StandaloneDatabaseContext databaseContext = createNewDatabaseContext( databaseId );
        databaseMap.put( databaseId, databaseContext );
        return databaseContext;
    }

    @Override
    protected StandaloneDatabaseContext createDatabaseContext( Database database, GraphDatabaseFacade facade )
    {
        return new StandaloneDatabaseContext( database, facade );
    }

    @Override
    public void dropDatabase( DatabaseId ignore )
    {
        throw new DatabaseManagementException( "Default database manager does not support database drop." );
    }

    @Override
    public void stopDatabase( DatabaseId ignore )
    {
        throw new DatabaseManagementException( "Default database manager does not support database stop." );
    }

    @Override
    public void startDatabase( DatabaseId databaseId )
    {
        throw new DatabaseManagementException( "Default database manager does not support starting databases." );
    }

    private void checkDatabaseLimit( DatabaseId databaseId )
    {
        if ( databaseMap.size() >= 2 )
        {
            throw new DatabaseManagementException( "Default database already exists. Fail to create another database: " + databaseId.name() );
        }
    }
}
