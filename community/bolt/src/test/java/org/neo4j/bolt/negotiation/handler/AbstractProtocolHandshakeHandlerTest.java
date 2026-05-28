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
package org.neo4j.bolt.negotiation.handler;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.logging.AssertableLogProvider;

public abstract class AbstractProtocolHandshakeHandlerTest {

    protected AssertableLogProvider logProvider;

    @BeforeEach
    void prepare() {
        this.logProvider = new AssertableLogProvider();
    }

    protected BoltProtocol newBoltProtocol(ProtocolVersion version) {
        BoltProtocol handler = mock(BoltProtocol.class);

        when(handler.version()).thenReturn(version);

        return handler;
    }

    protected BoltProtocolRegistry newProtocolFactory(ProtocolVersion version) {
        var protocol = newBoltProtocol(version);
        return newProtocolFactory(version, protocol);
    }

    protected BoltProtocolRegistry newProtocolFactory(ProtocolVersion version, BoltProtocol protocol) {
        var registry = mock(BoltProtocolRegistry.class);

        when(registry.get(eq(version))).thenReturn(Optional.of(protocol));

        return registry;
    }
}
