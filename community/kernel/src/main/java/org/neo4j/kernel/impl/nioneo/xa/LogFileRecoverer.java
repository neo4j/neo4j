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
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;

import static org.neo4j.kernel.impl.util.Cursors.exhaust;

public class LogFileRecoverer implements Visitor<ReadableLogChannel, IOException>
{
    private final VersionAwareLogEntryReader logEntryReader;
    private final Visitor<CommittedTransactionRepresentation, IOException> visitor;

    public LogFileRecoverer( VersionAwareLogEntryReader logEntryReader,
            Visitor<CommittedTransactionRepresentation, IOException> visitor )
    {
        this.logEntryReader = logEntryReader;
        this.visitor = visitor;
    }

    @Override
    public boolean visit( ReadableLogChannel channel ) throws IOException
    {
        // Intentionally don't close the cursor here since after recovery the channel is still used.
        // I dislike this exception to the rule though.
        exhaust( new PhysicalTransactionCursor( channel, logEntryReader, visitor ) );
        return true;
    }
}
