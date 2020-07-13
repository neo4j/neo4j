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
package org.neo4j.logging;

/**
 * A {@link LogProvider} implementation that duplicates all messages to other LogProvider instances
 */
public class DuplicatingLogProvider implements LogProvider
{
    private final LogProvider logProvider1;
    private final LogProvider logProvider2;

    public DuplicatingLogProvider( LogProvider logProvider1, LogProvider logProvider2 )
    {
        this.logProvider1 = logProvider1;
        this.logProvider2 = logProvider2;
    }

    @Override
    public Log getLog( Class<?> loggingClass )
    {
        return new DuplicatingLog( logProvider1.getLog( loggingClass ), logProvider2.getLog( loggingClass ) );
    }

    @Override
    public Log getLog( String name )
    {
        return new DuplicatingLog( logProvider1.getLog( name ), logProvider2.getLog( name ) );
    }
}
