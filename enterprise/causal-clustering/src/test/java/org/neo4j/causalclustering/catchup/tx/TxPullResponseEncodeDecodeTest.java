/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.tx;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class TxPullResponseEncodeDecodeTest
{
    @Test
    public void shouldEncodeAndDecodePullResponseMessage()
    {
        // given
        EmbeddedChannel channel = new EmbeddedChannel( new TxPullResponseEncoder(), new TxPullResponseDecoder() );
        TxPullResponse sent = new TxPullResponse( new StoreId( 1, 2, 3, 4 ), newCommittedTransactionRepresentation() );

        // when
        channel.writeOutbound( sent );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        TxPullResponse received = channel.readInbound();
        assertNotSame( sent, received );
        assertEquals( sent, received );
    }

    private CommittedTransactionRepresentation newCommittedTransactionRepresentation()
    {
        final long arbitraryRecordId = 27L;
        Command.NodeCommand command =
                new Command.NodeCommand( new NodeRecord( arbitraryRecordId ), new NodeRecord( arbitraryRecordId ) );

        PhysicalTransactionRepresentation physicalTransactionRepresentation =
                new PhysicalTransactionRepresentation( singletonList( new LogEntryCommand( command ).getCommand() ) );
        physicalTransactionRepresentation.setHeader( new byte[]{}, 0, 0, 0, 0, 0, 0 );

        LogEntryStart startEntry = new LogEntryStart( 0, 0, 0L, 0L, new byte[]{}, LogPosition.UNSPECIFIED );
        LogEntryCommit commitEntry = new LogEntryCommit( 42, 0 );

        return new CommittedTransactionRepresentation( startEntry, physicalTransactionRepresentation, commitEntry );
    }

}
