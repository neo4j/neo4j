/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Map;
import java.util.Optional;

import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Simple api for retrieving a human readable state for a given database, by id.
 *
 * Also provides the ability to check whether a database is in a failed state.
 * A failed database has usually failed to undergo some state transition (i.e. START -> STOP)
 */
public interface DatabaseStateService
{

    /**
     * Return a snapshot of states for all known databases.
     *
     * *NOTE* that if a database has been dropped it will be excluded from this table
     *
     * @return map of named database Ids to operator states
     */
    Map<NamedDatabaseId,DatabaseState> stateOfAllDatabases();

    /**
     * Note that if a database with the given name does not exist, the state
     * "UNKNOWN" will be returned.
     *
     * @param namedDatabaseId the database whose state to return
     * @return state of database with name
     */
    DatabaseState stateOfDatabase( NamedDatabaseId namedDatabaseId );

    /**
     * Note that if a database with the given name does not exist, {@code Optional.empty()}
     * will be returned.
     *
     * @param namedDatabaseId the database to check for failure
     * @return the cause of the database failure, if there is one.
     */
    Optional<Throwable> causeOfFailure( NamedDatabaseId namedDatabaseId );

}
