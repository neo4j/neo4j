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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.time.Instant;

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.storageengine.api.StoreId;

import static java.lang.Math.min;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToShort;
import static org.neo4j.kernel.impl.transaction.log.entry.CheckpointLogVersionSelector.LATEST;
import static org.neo4j.kernel.impl.transaction.log.entry.CheckpointParserSetV4_2.MAX_DESCRIPTION_LENGTH;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT;

public class DetachedCheckpointLogEntryWriter
{
    static final int RECORD_LENGTH_BYTES = 192;
    protected final WritableChecksumChannel channel;

    public DetachedCheckpointLogEntryWriter( WritableChecksumChannel channel )
    {
        this.channel = channel;
    }

    public void writeCheckPointEntry( LogPosition logPosition, Instant checkpointTime, StoreId storeId, String reason ) throws IOException
    {
        channel.beginChecksum();
        writeLogEntryHeader( DETACHED_CHECK_POINT, channel );
        byte[] reasonBytes = reason.getBytes();
        short length = safeCastIntToShort( min( reasonBytes.length, MAX_DESCRIPTION_LENGTH ) );
        byte[] descriptionBytes = new byte[MAX_DESCRIPTION_LENGTH];
        System.arraycopy( reasonBytes, 0, descriptionBytes, 0, length );
        channel.putLong( logPosition.getLogVersion() )
               .putLong( logPosition.getByteOffset() )
               .putLong( checkpointTime.toEpochMilli() )
               .putLong( storeId.getCreationTime() )
               .putLong( storeId.getRandomId() )
               .putLong( storeId.getStoreVersion() )
               .putLong( storeId.getUpgradeTime() )
               .putLong( storeId.getUpgradeTxId() )
               .putShort( length )
               .put( descriptionBytes, descriptionBytes.length );
        channel.putChecksum();
    }

    protected static void writeLogEntryHeader( byte type, WritableChannel channel ) throws IOException
    {
        channel.put( LATEST.versionByte() ).put( type );
    }
}
