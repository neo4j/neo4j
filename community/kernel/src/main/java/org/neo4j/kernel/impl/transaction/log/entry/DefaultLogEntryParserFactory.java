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
package org.neo4j.kernel.impl.transaction.log.entry;

public class DefaultLogEntryParserFactory implements LogEntryParserFactory
{
    // Remember the last version of the last dispatcher returned. Don't use a map because of it the overhead
    // of it. Typically lots of log have the same version comes together.
    private byte lastVersion;
    private LogEntryParserDispatcher lastParserDispatcher;

    @Override
    public LogEntryParserDispatcher newInstance( byte logVersion )
    {
        if ( logVersion == lastVersion && lastParserDispatcher != null )
        {
            return lastParserDispatcher;
        }

        lastVersion = logVersion;
        return (lastParserDispatcher = figureOutCorrectDispatcher( logVersion ));
    }

    private LogEntryParserDispatcher figureOutCorrectDispatcher( byte logVersion )
    {
        switch ( logVersion )
        {
            // These are not thread safe, so if they are to be cached it has to be done in an object pool
            case LogVersions.LOG_VERSION_1_9:
                return new LogEntryParserDispatcher<>( LogEntryParsersV2.values() );
            case LogVersions.LOG_VERSION_2_0:
                return new LogEntryParserDispatcher<>( LogEntryParsersV3.values() );
            case LogVersions.LOG_VERSION_2_1:
                return new LogEntryParserDispatcher<>( LogEntryParsersV4.values() );
            case LogVersions.LOG_VERSION_2_2:
                return new LogEntryParserDispatcher<>( LogEntryParsersV5.values() );
            default:
                throw new IllegalStateException( "Unsupported log version format " + logVersion );
        }
    }
}
