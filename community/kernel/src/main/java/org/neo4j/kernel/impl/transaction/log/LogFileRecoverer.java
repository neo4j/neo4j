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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.helpers.collection.CloseableVisitor;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.state.RecoverableTransaction;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;

public class LogFileRecoverer implements Visitor<LogVersionedStoreChannel,IOException>
{
    private final LogEntryReader<ReadableLogChannel> logEntryReader;
    private final CloseableVisitor<RecoverableTransaction,IOException> visitor;

    public LogFileRecoverer( LogEntryReader<ReadableLogChannel> logEntryReader,
            CloseableVisitor<RecoverableTransaction,IOException> visitor )
    {
        this.logEntryReader = logEntryReader;
        this.visitor = visitor;
    }

    @Override
    public boolean visit( LogVersionedStoreChannel channel ) throws IOException
    {
        final ReadableVersionableLogChannel recoveredDataChannel =
                new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );

        try ( final PhysicalTransactionCursor<ReadableLogChannel> physicalTransactionCursor =
                      new PhysicalTransactionCursor<>( recoveredDataChannel, logEntryReader ) )
        {
            RecoverableTransaction recoverableTransaction = new RecoverableTransaction()
            {
                @Override
                public CommittedTransactionRepresentation representation()
                {
                    return physicalTransactionCursor.get();
                }

                @Override
                public LogPosition positionAfterTx()
                {
                    long version = recoveredDataChannel.getVersion();
                    long byteOffset = physicalTransactionCursor.lastKnownGoodPosition();
                    return new LogPosition( version, byteOffset );
                }
            };

            while ( physicalTransactionCursor.next() && !visitor.visit( recoverableTransaction ) )
            {
            }

            // Now that all ok transactions have been read, if needed truncate the position to cut
            // off any potentially broken transactions
            long lastKnownGoodPosition = physicalTransactionCursor.lastKnownGoodPosition();
            if ( channel.position() > lastKnownGoodPosition )
            {
                channel.truncate( lastKnownGoodPosition );
            }
        }

        visitor.close();
        return true;
    }
}
