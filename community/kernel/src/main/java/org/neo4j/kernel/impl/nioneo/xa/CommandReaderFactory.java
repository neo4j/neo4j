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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoCommandReaderV0;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoCommandReaderV1;

public interface CommandReaderFactory
{
    CommandReader newInstance( byte logEntryVersion );

    public static final CommandReaderFactory DEFAULT = new CommandReaderFactory()
    {
        @Override
        public CommandReader newInstance( byte logEntryVersion )
        {
            switch ( logEntryVersion )
            {
                // These are not thread safe, so if they are to be cached it has to be done in an object pool
                case 0:
                    return new PhysicalLogNeoCommandReaderV0();
                case -1:
                    return new PhysicalLogNeoCommandReaderV1();
                default:
                    throw new IllegalArgumentException( "Unknown log entry version " + logEntryVersion );
            }
        }
    };
}
