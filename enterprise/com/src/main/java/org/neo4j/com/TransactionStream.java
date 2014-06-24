/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.IOCursor;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;

import static org.neo4j.kernel.impl.util.Cursors.exhaust;

/**
 * Represents a stream of the data of one or more consecutive transactions.
 */
public class TransactionStream implements Visitor<ChannelBuffer, IOException>
{
    public static final TransactionStream EMPTY = new TransactionStream()
    {
        @Override
        public boolean visit( ChannelBuffer element ) throws IOException
        {
            return false;
        }
    };

    private final IOCursor transactionCursor;
    private ChannelBuffer toStreamTo;

    private TransactionStream()
    {
        this.transactionCursor = null;
    }

    public TransactionStream( LogicalTransactionStore txStore, long transactionIdToStreamFrom ) throws IOException
    {
        this.transactionCursor = txStore.getCursor( transactionIdToStreamFrom, new TransactionThing() );
    }

    @Override
    public boolean visit( ChannelBuffer element ) throws IOException
    {
        toStreamTo = element;
        exhaust( transactionCursor );
        return true;
    }

    private class TransactionThing implements Visitor<CommittedTransactionRepresentation, IOException>
    {
        @Override
        public boolean visit( CommittedTransactionRepresentation element ) throws IOException
        {
            new Protocol.CommittedTransactionRepresentationSerializer( element ).write( toStreamTo );
            return true;
        }
    }
}
