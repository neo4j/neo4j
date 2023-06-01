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

import java.util.Map;
import org.neo4j.bolt.protocol.v53.BoltProtocolV53;

public class BoltV53Wire extends BoltV52Wire {
    public BoltV53Wire() {
        super(BoltProtocolV53.VERSION);
    }

    @Override
    public String getUserAgent() {
        return "BoltWire/5.3";
    }

    @Override
    public Map<String, String> getBoltAgent() {
        return Map.of("product", "bolt-wire/5.3");
    }
}
