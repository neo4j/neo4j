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
package org.neo4j.server.queryapi.driver.boltmessage;

import io.netty.channel.ChannelPipeline;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.netty.impl.async.connection.ChannelPipelineBuilder;
import org.neo4j.bolt.connection.netty.impl.async.inbound.ChannelErrorHandler;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageFormat;
import org.neo4j.bolt.connection.values.ValueFactory;
import org.neo4j.server.queryapi.driver.boltmessage.pipeline.InboundMessageDecoder;
import org.neo4j.server.queryapi.driver.boltmessage.pipeline.InboundMessageReader;
import org.neo4j.server.queryapi.driver.boltmessage.pipeline.OutboundMessageEncoder;
import org.neo4j.server.queryapi.driver.boltmessage.pipeline.OutboundMessageWriter;

public class BoltMessagePipelineBuilder implements ChannelPipelineBuilder {
    @Override
    public void build(
            MessageFormat messageFormat, ChannelPipeline pipeline, LoggingProvider logging, ValueFactory valueFactory) {
        pipeline.addLast(new InboundMessageDecoder());
        pipeline.addLast(new InboundMessageReader());
        pipeline.addLast(new OutboundMessageEncoder());
        pipeline.addLast(new OutboundMessageWriter(messageFormat, logging, valueFactory));
        pipeline.addLast(new ChannelErrorHandler(logging));
    }
}
