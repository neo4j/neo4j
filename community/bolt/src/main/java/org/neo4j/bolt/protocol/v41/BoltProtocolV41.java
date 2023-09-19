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
package org.neo4j.bolt.protocol.v41;

import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.bolt.protocol.v41.message.decoder.authentication.HelloMessageDecoderV41;
import org.neo4j.packstream.signal.FrameSignal;
import org.neo4j.packstream.struct.StructRegistry;

/**
 * Bolt protocol V4.1 It hosts all the components that are specific to BoltV4.1
 */
public class BoltProtocolV41 extends BoltProtocolV40 {
    private static final BoltProtocolV41 INSTANCE = new BoltProtocolV41();
    public static final ProtocolVersion VERSION = new ProtocolVersion(4, 1);

    protected BoltProtocolV41() {}

    public static BoltProtocolV41 getInstance() {
        return INSTANCE;
    }

    @Override
    public Set<Feature> features() {
        return Collections.emptySet();
    }

    @Override
    public Predicate<FrameSignal> frameSignalFilter() {
        // all signals are supported
        return signal -> false;
    }

    @Override
    protected StructRegistry.Builder<Connection, RequestMessage> createRequestMessageRegistry() {
        return super.createRequestMessageRegistry().register(HelloMessageDecoderV41.getInstance());
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }
}
