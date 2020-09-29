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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryDetachedCheckpoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryInlinedCheckPoint;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertSame;

class CheckpointInfoTest
{
    @Test
    void checkpointInfoOfLegacyCheckpointEntry()
    {
        var logPosition = new LogPosition( 0, 1 );
        StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );
        LogPosition position = new LogPosition( 1, 2 );
        var checkpointInfo = new CheckpointInfo( new LogEntryInlinedCheckPoint( logPosition ), storeId, position );

        assertSame( logPosition, checkpointInfo.getTransactionLogPosition() );
        assertSame( storeId, checkpointInfo.storeId() );
        assertSame( position, checkpointInfo.getCheckpointEntryPosition() );
    }

    @Test
    void checkpointInfoOfDetachedCheckpointEntry()
    {
        var logPosition = new LogPosition( 0, 1 );
        var storeId = new StoreId( 3, 4, 5, 6, 7 );
        LogPosition position = new LogPosition( 1, 2 );
        var checkpointInfo = new CheckpointInfo( new LogEntryDetachedCheckpoint( (byte) 1, logPosition, 2, storeId, "checkpoint" ), position );

        assertSame( logPosition, checkpointInfo.getTransactionLogPosition() );
        assertSame( storeId, checkpointInfo.storeId() );
        assertSame( position, checkpointInfo.getCheckpointEntryPosition() );
    }
}
