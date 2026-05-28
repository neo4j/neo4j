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
package org.neo4j.bolt.testing.messages;

import io.netty.buffer.ByteBuf;
import java.util.function.UnaryOperator;
import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.v57.BoltProtocolV57;
import org.neo4j.bolt.testing.messages.factory.TelemetryMessageBuilder;

public class BoltV57Wire extends BoltV56Wire {

    protected BoltV57Wire(ProtocolVersion version) {
        super(version);
    }

    public BoltV57Wire() {
        this(BoltProtocolV57.VERSION);
    }

    @Override
    public String getUserAgent() {
        return "BoltWire/5.7";
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return super.getProtocolVersion();
    }

    @Override
    public ByteBuf telemetry(UnaryOperator<TelemetryMessageBuilder> fn) {
        return fn.apply(new TelemetryMessageBuilder(this.getProtocolVersion())).build();
    }

    @Override
    public boolean hasGQLStatus() {
        return true;
    }
}
