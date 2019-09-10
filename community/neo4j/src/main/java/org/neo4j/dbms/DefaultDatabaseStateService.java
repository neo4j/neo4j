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
package org.neo4j.dbms;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DefaultDatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Database State Service for the community edition of the dbms
 */
public final class DefaultDatabaseStateService implements DatabaseStateService
{
    private final DatabaseManager<StandaloneDatabaseContext> databaseManager;

    public DefaultDatabaseStateService( DefaultDatabaseManager databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    @Override
    public OperatorState stateOfDatabase( NamedDatabaseId namedDatabaseId )
    {
        return databaseManager.getDatabaseContext( namedDatabaseId )
                .map( ctx ->
                        new CommunityDatabaseState( ctx.database().getNamedDatabaseId(),
                                ctx.database().isStarted(),
                                ctx.isFailed(),
                                ctx.failureCause() ).operatorState() )
                .orElse( DefaultOperatorState.UNKNOWN );
    }

    @Override
    public Optional<Throwable> causeOfFailure( NamedDatabaseId namedDatabaseId )
    {
        return databaseManager.getDatabaseContext( namedDatabaseId ).map( StandaloneDatabaseContext::failureCause );
    }
}
