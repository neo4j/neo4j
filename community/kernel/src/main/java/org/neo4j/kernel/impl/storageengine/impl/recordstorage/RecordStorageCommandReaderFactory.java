/*
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV1_9;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_0;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_1;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2_4;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV3_0;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static java.lang.Math.abs;

public class RecordStorageCommandReaderFactory implements CommandReaderFactory
{
    // All supported readers. Key/index is LogEntryVersion byte code.
    private final CommandReader[] readersNegative, readersNeutral;

    public RecordStorageCommandReaderFactory()
    {
        // These versions have version=0, but are distinguished by the log header format version
        readersNeutral = new CommandReader[10]; // pessimistic size
        readersNeutral[LogEntryVersion.V1_9.logHeaderFormatVersion()] = new PhysicalLogCommandReaderV1_9();
        readersNeutral[LogEntryVersion.V2_0.logHeaderFormatVersion()] = new PhysicalLogCommandReaderV2_0();

        // These versions have each their own version and so are directly distinguishable
        readersNegative = new CommandReader[10]; // pessimistic size
        readersNegative[-LogEntryVersion.V2_1.byteCode()] = new PhysicalLogCommandReaderV2_1();
        readersNegative[-LogEntryVersion.V2_2.byteCode()] = new PhysicalLogCommandReaderV2_2();
        readersNegative[-LogEntryVersion.V2_2_4.byteCode()] = new PhysicalLogCommandReaderV2_2_4();
        readersNegative[-LogEntryVersion.V2_3.byteCode()] = new PhysicalLogCommandReaderV2_2_4();
        readersNegative[-LogEntryVersion.V3_0.byteCode()] = new PhysicalLogCommandReaderV3_0();

        // A little extra safety check so that we got 'em all
        LogEntryVersion[] versions = LogEntryVersion.values();
        for ( int i = 0; i < versions.length; i++ )
        {
            if ( versions[i] == null )
            {
                throw new IllegalStateException( "Version " + versions[i] + " not handled" );
            }
        }
    }

    @Override
    public CommandReader byVersion( byte version, byte legacyHeaderVersion )
    {
        CommandReader[] readers = null;
        byte key = 0;
        if ( version == 0 )
        {
            readers = readersNeutral;
            key = legacyHeaderVersion;
        }
        else if ( version < 0 )
        {
            readers = readersNegative;
            key = (byte) abs( version );
        }

        if ( readers == null || key >= readers.length )
        {
            throw new IllegalArgumentException( "Unsupported version:" + version +
                    " headerVersion:" + legacyHeaderVersion );
        }
        return readers[key];
    }
}
