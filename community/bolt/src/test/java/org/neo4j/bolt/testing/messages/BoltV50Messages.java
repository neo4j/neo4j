/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.bolt.testing.messages;

import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.v50.BoltProtocolV50;

public class BoltV50Messages extends BoltV44Messages {
    private static final String USER_AGENT = "BoltV51Wire/0.0";

    private static final BoltV50Messages INSTANCE = new BoltV50Messages();

    protected BoltV50Messages() {}

    public static BoltMessages getInstance() {
        return INSTANCE;
    }

    @Override
    public ProtocolVersion version() {
        return BoltProtocolV50.VERSION;
    }
}
