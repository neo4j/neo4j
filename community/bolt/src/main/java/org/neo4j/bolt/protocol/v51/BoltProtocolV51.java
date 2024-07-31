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
package org.neo4j.bolt.protocol.v51;

import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.AbstractBoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.message.decoder.generic.TelemetryMessageDecoder;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v44.fsm.response.metadata.MetadataHandlerV44;
import org.neo4j.bolt.protocol.v44.message.decoder.transaction.RunMessageDecoderV44;
import org.neo4j.bolt.protocol.v50.message.decoder.transaction.BeginMessageDecoderV50;
import org.neo4j.bolt.protocol.v51.message.decoder.authentication.HelloMessageDecoderV51;
import org.neo4j.packstream.struct.StructRegistry;

public final class BoltProtocolV51 extends AbstractBoltProtocol {
    private static final BoltProtocolV51 INSTANCE = new BoltProtocolV51();
    public static final ProtocolVersion VERSION = new ProtocolVersion(5, 1);

    private BoltProtocolV51() {}

    public static BoltProtocolV51 getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    protected StructRegistry.Builder<Connection, RequestMessage> createRequestMessageRegistry() {
        return super.createRequestMessageRegistry()
                // Authentication
                .register(HelloMessageDecoderV51.getInstance())
                // Transaction
                .register(BeginMessageDecoderV50.getInstance())
                .register(RunMessageDecoderV44.getInstance())
                // Generic
                .unregister(TelemetryMessageDecoder.getInstance());
    }

    @Override
    public MetadataHandler metadataHandler() {
        return MetadataHandlerV44.getInstance();
    }
}
