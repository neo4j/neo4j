/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionCursor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.util.Consumer;

public class LogFileRecoverer implements Visitor<ReadableLogChannel, IOException>
{
    private final VersionAwareLogEntryReader logEntryReader;
    private final Consumer<TransactionRepresentation, IOException> consumer;

    public LogFileRecoverer( VersionAwareLogEntryReader logEntryReader, Consumer<TransactionRepresentation,
            IOException> consumer )
    {
        this.logEntryReader = logEntryReader;
        this.consumer = consumer;
    }

    @Override
    public boolean visit( ReadableLogChannel channel ) throws IOException
    {
        TransactionCursor cursor = new PhysicalTransactionCursor( channel, logEntryReader );
        while ( cursor.next( consumer ) )
        {
            // Just go through the recovery data, handing it on to the consumer.
        }
        return true;
    }
}
