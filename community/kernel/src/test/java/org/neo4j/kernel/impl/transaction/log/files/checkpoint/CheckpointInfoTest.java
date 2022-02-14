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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryDetachedCheckpointV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TRANSACTION_ID;

class CheckpointInfoTest
{
    @Test
    void checkpointInfoOfDetachedCheckpoint42Entry()
    {
        var logPosition = new LogPosition( 0, 1 );
        var storeId = new StoreId( 3, 4, 5 );
        LogPosition position = new LogPosition( 1, 2 );
        LogPosition positionAfterCheckpoint = new LogPosition( 3, 4 );
        LogPosition postReaderPosition = new LogPosition( 5, 6 );
        var checkpointInfo =
                CheckpointInfo.ofLogEntry( new LogEntryDetachedCheckpointV4_2( KernelVersion.LATEST, logPosition, 2, storeId, "checkpoint" ), position,
                        positionAfterCheckpoint, postReaderPosition );

        assertSame( logPosition, checkpointInfo.getTransactionLogPosition() );
        assertSame( storeId, checkpointInfo.storeId() );
        assertSame( position, checkpointInfo.getCheckpointEntryPosition() );
        assertSame( positionAfterCheckpoint, checkpointInfo.getChannelPositionAfterCheckpoint() );
        assertSame( postReaderPosition, checkpointInfo.getCheckpointFilePostReadPosition() );
        assertEquals( UNKNOWN_TRANSACTION_ID, checkpointInfo.getTransactionId() );
    }

    @Test
    void checkpointInfoOfDetachedCheckpoint50Entry()
    {
        var logPosition = new LogPosition( 0, 1 );
        var storeId = new StoreId( 3, 4, 5 );
        LogPosition position = new LogPosition( 1, 2 );
        LogPosition positionAfterCheckpoint = new LogPosition( 3, 4 );
        LogPosition postReaderPosition = new LogPosition( 5, 6 );
        TransactionId transactionId = new TransactionId( 6, 7, 8 );
        var checkpointInfo =
                CheckpointInfo.ofLogEntry( new LogEntryDetachedCheckpointV5_0( KernelVersion.LATEST, transactionId, logPosition, 2, storeId, "checkpoint" ),
                        position, positionAfterCheckpoint, postReaderPosition );

        assertSame( logPosition, checkpointInfo.getTransactionLogPosition() );
        assertSame( storeId, checkpointInfo.storeId() );
        assertSame( position, checkpointInfo.getCheckpointEntryPosition() );
        assertSame( transactionId, checkpointInfo.getTransactionId() );
        assertSame( positionAfterCheckpoint, checkpointInfo.getChannelPositionAfterCheckpoint() );
        assertSame( postReaderPosition, checkpointInfo.getCheckpointFilePostReadPosition() );
    }
}
