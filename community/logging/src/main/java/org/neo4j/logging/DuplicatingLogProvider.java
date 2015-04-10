/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.ArrayList;

/**
 * A {@link LogProvider} implementation that duplicates all messages to other LogProvider instances
 */
public class DuplicatingLogProvider implements LogProvider
{
    private final LogProvider[] logProviders;

    /**
     * @param logProviders A list of {@link LogProvider} instances that messages should be duplicated to
     */
    public DuplicatingLogProvider( LogProvider... logProviders )
    {
        this.logProviders = logProviders;
    }

    @Override
    public Log getLog( Class loggingClass )
    {
        ArrayList<Log> logs = new ArrayList<>();
        for ( LogProvider logProvider : logProviders )
        {
            logs.add( logProvider.getLog( loggingClass ) );
        }
        return new DuplicatingLog( logs );
    }

    @Override
    public Log getLog( String category )
    {
        ArrayList<Log> logs = new ArrayList<>();
        for ( LogProvider logProvider : logProviders )
        {
            logs.add( logProvider.getLog( category ) );
        }
        return new DuplicatingLog( logs );
    }
}
