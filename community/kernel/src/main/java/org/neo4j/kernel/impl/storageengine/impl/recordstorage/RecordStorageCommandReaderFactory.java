/*
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.neo4j.kernel.impl.storageengine.CommandReaderFactory;
import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV1_9;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_0;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_1;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2_4;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV3_0;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;

public class RecordStorageCommandReaderFactory implements CommandReaderFactory
{
    // All supported readers. Key/index is LogEntryVersion ordinal.
    private final CommandReader[] readers;

    public RecordStorageCommandReaderFactory()
    {
        LogEntryVersion[] versions = LogEntryVersion.values();
        readers = new CommandReader[versions.length];
        readers[LogEntryVersion.V1_9.ordinal()] = new PhysicalLogCommandReaderV1_9();
        readers[LogEntryVersion.V2_0.ordinal()] = new PhysicalLogCommandReaderV2_0();
        readers[LogEntryVersion.V2_1.ordinal()] = new PhysicalLogCommandReaderV2_1();
        readers[LogEntryVersion.V2_2.ordinal()] = new PhysicalLogCommandReaderV2_2();
        readers[LogEntryVersion.V2_2_4.ordinal()] = new PhysicalLogCommandReaderV2_2_4();
        readers[LogEntryVersion.V2_3.ordinal()] = new PhysicalLogCommandReaderV2_2_4();
        readers[LogEntryVersion.V3_0.ordinal()] = new PhysicalLogCommandReaderV3_0();
        for ( int i = 0; i < versions.length; i++ )
        {
            if ( versions[i] == null )
            {
                throw new IllegalStateException( "Version " + versions[i] + " not handled" );
            }
        }
    }

    @Override
    public CommandReader byVersion( LogEntryVersion version )
    {
        int key = version.ordinal();
        if ( key >= readers.length )
        {
            throw new IllegalArgumentException( "Unsupported entry version " + version );
        }
        return readers[key];
    }
}
