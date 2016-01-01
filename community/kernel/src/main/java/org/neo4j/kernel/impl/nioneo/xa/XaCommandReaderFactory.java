/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandReaderV0;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandReaderV1;

public interface XaCommandReaderFactory
{
    XaCommandReader newInstance( byte logEntryVersion, ByteBuffer scratch );

    public static final XaCommandReaderFactory DEFAULT = new XaCommandReaderFactory()
    {
        @Override
        public XaCommandReader newInstance( byte logEntryVersion, ByteBuffer scratch )
        {
            switch ( logEntryVersion )
            {
                // These are not thread safe, so if they are to be cached it has to be done in an object pool
                case 0:
                    return new PhysicalLogNeoXaCommandReaderV0( scratch );
                case -1:
                    return new PhysicalLogNeoXaCommandReaderV1( scratch );
                default:
                    throw new IllegalArgumentException( "Unknown log entry version " + logEntryVersion );
            }
        }
    };
}
