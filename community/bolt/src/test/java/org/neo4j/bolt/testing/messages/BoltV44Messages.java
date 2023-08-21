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

import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;

public class BoltV44Messages extends BoltV43Messages {
    private static final BoltV44Messages INSTANCE = new BoltV44Messages();

    protected BoltV44Messages() {}

    public static BoltMessages getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return BoltProtocolV44.VERSION;
    }

    @Override
    public RequestMessage authenticate(String principal, String credentials) {
        return this.hello(principal, credentials);
    }

    @Override
    public RequestMessage logon() {
        throw new UnsupportedOperationException("Logon");
    }

    @Override
    public RequestMessage logon(String principal, String credentials) {
        throw new UnsupportedOperationException("Logon");
    }

    @Override
    public RequestMessage logoff() {
        throw new UnsupportedOperationException("Logoff");
    }
}
