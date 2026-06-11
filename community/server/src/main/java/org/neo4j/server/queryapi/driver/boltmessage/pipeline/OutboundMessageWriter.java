/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.queryapi.driver.boltmessage.pipeline;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.netty.impl.async.outbound.OutboundMessageHandler;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageFormat;
import org.neo4j.bolt.connection.values.ValueFactory;

public class OutboundMessageWriter extends OutboundMessageHandler {

    public OutboundMessageWriter(MessageFormat messageFormat, LoggingProvider logging, ValueFactory valueFactory) {
        super(messageFormat, logging, valueFactory);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) {
        out.add(msg);
    }
}
