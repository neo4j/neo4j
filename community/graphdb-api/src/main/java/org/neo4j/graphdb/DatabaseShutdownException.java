/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb;

import org.neo4j.kernel.api.exceptions.Status;

public class DatabaseShutdownException extends RuntimeException implements Status.HasStatus
{
    public DatabaseShutdownException( )
    {
        super( "This database is shutdown." );
    }

    public DatabaseShutdownException( String message )
    {
        super( message );
    }

    @Override
    public Status status()
    {
        return Status.General.DatabaseUnavailable;
    }
}
