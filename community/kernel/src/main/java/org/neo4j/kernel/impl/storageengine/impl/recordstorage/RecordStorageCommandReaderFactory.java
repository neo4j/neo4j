/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2_10;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2_4;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV3_0;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV3_0_2;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static java.lang.Math.abs;

public class RecordStorageCommandReaderFactory implements CommandReaderFactory
{
    // All supported readers. Key/index is LogEntryVersion byte code.
    private final CommandReader[] readers;

    public RecordStorageCommandReaderFactory()
    {
        readers = new CommandReader[11]; // pessimistic size
        readers[-LogEntryVersion.V2_3.byteCode()] = new PhysicalLogCommandReaderV2_2_4();
        readers[-LogEntryVersion.V3_0.byteCode()] = new PhysicalLogCommandReaderV3_0();
        readers[-LogEntryVersion.V2_3_5.byteCode()] = new PhysicalLogCommandReaderV2_2_10();
        readers[-LogEntryVersion.V3_0_2.byteCode()] = new PhysicalLogCommandReaderV3_0_2();
        // The 3_0_10 version bump is only to prevent mixed-version clusters; format is otherwise backwards compatible.
        readers[-LogEntryVersion.V3_0_10.byteCode()] = new PhysicalLogCommandReaderV3_0_2();

        // A little extra safety check so that we got 'em all
        LogEntryVersion[] versions = LogEntryVersion.values();
        for ( LogEntryVersion version : versions )
        {
            CommandReader versionReader = readers[abs( version.byteCode() )];
            if ( versionReader == null )
            {
                throw new IllegalStateException( "Version " + version + " not handled" );
            }
        }
    }

    @Override
    public CommandReader byVersion( byte version )
    {
        byte key = (byte) abs( version );
        if ( key >= readers.length )
        {
            throw new IllegalArgumentException( "Unsupported version:" + version );
        }
        return readers[key];
    }
}
