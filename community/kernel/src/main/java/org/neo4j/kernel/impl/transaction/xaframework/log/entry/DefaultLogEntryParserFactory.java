/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework.log.entry;

public class DefaultLogEntryParserFactory implements LogEntryParserFactory
{
    @Override
    public LogEntryParserDispatcher newInstance( byte logVersion )
    {
        switch ( logVersion )
        {
            case LogVersions.LOG_VERSION_2_1:
                return new LogEntryParserDispatcherV4();
            case LogVersions.LOG_VERSION_2_2:
                return new LogEntryParserDispatcherV5();
            default:
                throw new IllegalStateException( "Unsupported log version format " + logVersion );
        }
    }

}
