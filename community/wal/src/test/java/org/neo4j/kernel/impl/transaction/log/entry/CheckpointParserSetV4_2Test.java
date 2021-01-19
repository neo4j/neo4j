/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT;

class CheckpointParserSetV4_2Test
{
    private final CommandReaderFactory commandReader = new TestCommandReaderFactory();
    private final LogPositionMarker positionMarker = new LogPositionMarker();

    @Test
    void parseDetachedCheckpointRecord() throws IOException
    {
        KernelVersion version = KernelVersion.V4_2;
        var storeId = new StoreId( 4, 5, 6, 7, 8 );
        var channel = new InMemoryClosableChannel();
        int checkpointMillis = 3;
        String checkpointDescription = "checkpoint";
        byte[] bytes = Arrays.copyOf( checkpointDescription.getBytes(), 120 );
        var checkpoint = new LogEntryDetachedCheckpoint( version, new LogPosition( 1, 2 ), checkpointMillis, storeId, checkpointDescription );

        channel.putLong( checkpoint.getLogPosition().getLogVersion() )
               .putLong( checkpoint.getLogPosition().getByteOffset() )
               .putLong( checkpointMillis )
               .putLong( storeId.getCreationTime() )
               .putLong( storeId.getRandomId() )
               .putLong( storeId.getStoreVersion() )
               .putLong( storeId.getUpgradeTime() )
               .putLong( storeId.getUpgradeTxId() )
               .putShort( (short) checkpointDescription.getBytes().length )
               .put( bytes, bytes.length );
        channel.putChecksum();

        var checkpointParser = LogEntryParserSets.checkpointParserSet( version ).select( DETACHED_CHECK_POINT );
        LogEntry logEntry = checkpointParser.parse( version, channel, positionMarker, commandReader );
        assertEquals( checkpoint, logEntry );
    }
}
