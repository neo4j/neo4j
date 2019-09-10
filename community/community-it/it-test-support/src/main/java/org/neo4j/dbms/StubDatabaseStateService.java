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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.neo4j.kernel.database.NamedDatabaseId;

import static org.neo4j.dbms.DefaultOperatorState.UNKNOWN;

public class StubDatabaseStateService implements DatabaseStateService
{
    private final Map<NamedDatabaseId,DatabaseState> databaseStates;

    public StubDatabaseStateService()
    {
        this.databaseStates = Collections.emptyMap();
    }

    public StubDatabaseStateService( Map<NamedDatabaseId,DatabaseState> databaseStates )
    {
        this.databaseStates = databaseStates;
    }

    @Override
    public OperatorState stateOfDatabase( NamedDatabaseId namedDatabaseId )
    {
        var state = databaseStates.get( namedDatabaseId );
        return state == null ? UNKNOWN : state.operatorState();
    }

    @Override
    public Optional<Throwable> causeOfFailure( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( databaseStates.get( namedDatabaseId ) ).flatMap( DatabaseState::failure );
    }
}
