/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;

/**
 * Serialized {@link CommittedTransactionRepresentation transactions} to raw bytes on the {@link ChannelBuffer
 * network}.
 * One serializer can be instantiated per response and is able to serialize one or many transactions.
 */
public class CommittedTransactionSerializer implements Visitor<CommittedTransactionRepresentation,Exception>
{
    private final LogEntryWriter writer;

    public CommittedTransactionSerializer( FlushableChannel networkFlushableChannel )
    {
        this.writer = new LogEntryWriter( networkFlushableChannel );
    }

    @Override
    public boolean visit( CommittedTransactionRepresentation tx ) throws IOException
    {
        writer.serialize( tx );
        return false;
    }
}
