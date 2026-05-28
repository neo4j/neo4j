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

import org.neo4j.bolt.negotiation.version.ProtocolVersion;
import org.neo4j.bolt.protocol.v60.BoltProtocolV60;
import org.neo4j.gqlstatus.ErrorMessageHolder;

// FIXME: Should not inherit from 5.x
public class BoltV60Wire extends BoltV58Wire {

    protected BoltV60Wire(ProtocolVersion version) {
        super(version);
    }

    public BoltV60Wire() {
        this(BoltProtocolV60.VERSION);
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return super.getProtocolVersion();
    }

    @Override
    public String getUserAgent() {
        return "BoltWire/6.0";
    }

    @Override
    public boolean hasLegacyFailureMessages() {
        // GQL messages as default are currently feature flagged - the exact version this is
        // released on is still subject to change
        return !ErrorMessageHolder.USE_NEW_ERROR_MESSAGES;
    }
}
