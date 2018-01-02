/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;

/**
 * Serialized {@link CommittedTransactionRepresentation transactions} to raw bytes on the {@link ChannelBuffer
 * network}.
 * One serializer can be instantiated per response and is able to serialize one or many transactions.
 */
public class CommittedTransactionSerializer implements Visitor<CommittedTransactionRepresentation,IOException>
{
    private final NetworkWritableLogChannel channel;
    private final LogEntryWriter writer;

    public CommittedTransactionSerializer( ChannelBuffer targetBuffer )
    {
        this.channel = new NetworkWritableLogChannel( targetBuffer );
        this.writer = new LogEntryWriter( channel, new CommandWriter( channel ) );
    }

    @Override
    public boolean visit( CommittedTransactionRepresentation tx ) throws IOException
    {
        LogEntryStart startEntry = tx.getStartEntry();
        writer.writeStartEntry( startEntry.getMasterId(), startEntry.getLocalId(),
                startEntry.getTimeWritten(), startEntry.getLastCommittedTxWhenTransactionStarted(),
                startEntry.getAdditionalHeader() );
        writer.serialize( tx.getTransactionRepresentation() );
        LogEntryCommit commitEntry = tx.getCommitEntry();
        writer.writeCommitEntry( commitEntry.getTxId(), commitEntry.getTimeWritten() );
        return false;
    }
}
