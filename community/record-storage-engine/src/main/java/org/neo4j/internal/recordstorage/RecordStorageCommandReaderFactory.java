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
package org.neo4j.internal.recordstorage;

import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class RecordStorageCommandReaderFactory implements CommandReaderFactory
{
    public static final CommandReaderFactory INSTANCE = new RecordStorageCommandReaderFactory();

    @Override
    public CommandReader get( int logEntryVersion )
    {
        // Historically the log entry version

        switch ( logEntryVersion )
        {
        case PhysicalLogCommandReaderV3_0_10.FORMAT_ID:
            return PhysicalLogCommandReaderV3_0_10.INSTANCE;
        case PhysicalLogCommandReaderV4_0.FORMAT_ID:
            return PhysicalLogCommandReaderV4_0.INSTANCE;
        case PhysicalLogCommandReaderV4_2.FORMAT_ID:
            return PhysicalLogCommandReaderV4_2.INSTANCE;
        default:
            throw new IllegalArgumentException( "Unsupported command format [id=" + logEntryVersion + "]" );
        }
    }
}
