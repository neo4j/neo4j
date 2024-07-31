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
package org.neo4j.bolt.protocol.v44;

import java.util.Collections;
import java.util.Set;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.message.decoder.connection.DefaultRouteMessageDecoder;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v43.BoltProtocolV43;
import org.neo4j.bolt.protocol.v44.fsm.response.metadata.MetadataHandlerV44;
import org.neo4j.bolt.protocol.v44.message.decoder.transaction.BeginMessageDecoderV44;
import org.neo4j.bolt.protocol.v44.message.decoder.transaction.RunMessageDecoderV44;
import org.neo4j.packstream.struct.StructRegistry;

/**
 * Bolt protocol V4.4 It hosts all the components that are specific to Bolt V4.4
 */
public final class BoltProtocolV44 extends BoltProtocolV43 {
    private static final BoltProtocolV44 INSTANCE = new BoltProtocolV44();
    public static final ProtocolVersion VERSION = new ProtocolVersion(4, 4);

    private BoltProtocolV44() {}

    public static BoltProtocolV44 getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    public Set<Feature> features() {
        return Collections.emptySet();
    }

    @Override
    protected StructRegistry.Builder<Connection, RequestMessage> createRequestMessageRegistry() {
        return super.createRequestMessageRegistry()
                .register(BeginMessageDecoderV44.getInstance())
                .register(DefaultRouteMessageDecoder.getInstance())
                .register(RunMessageDecoderV44.getInstance());
    }

    @Override
    public MetadataHandler metadataHandler() {
        return MetadataHandlerV44.getInstance();
    }
}
