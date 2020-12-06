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
package org.neo4j.shell;

import org.neo4j.shell.exception.CommandException;

/**
 * An object capable of tracking the active database.
 */
public interface DatabaseManager
{
    String ABSENT_DB_NAME = "";
    String SYSTEM_DB_NAME = "system";
    String DEFAULT_DEFAULT_DB_NAME = "neo4j";

    String DATABASE_UNAVAILABLE_ERROR_CODE = "Neo.TransientError.General.DatabaseUnavailable";

    /**
     * Sets the active database name as set by the user. If the current state is connected, try to reconnect to that database. If the current state is
     * disconnected, simply update `activeDatabaseAsSetByUser`.
     */
    void setActiveDatabase( String databaseName ) throws CommandException;

    String getActiveDatabaseAsSetByUser();

    String getActualDatabaseAsReportedByServer();
}
