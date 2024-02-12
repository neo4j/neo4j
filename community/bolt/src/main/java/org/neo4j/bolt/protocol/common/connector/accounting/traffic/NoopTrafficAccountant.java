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
package org.neo4j.bolt.protocol.common.connector.accounting.traffic;

public final class NoopTrafficAccountant implements TrafficAccountant {
    private static final NoopTrafficAccountant INSTANCE = new NoopTrafficAccountant();

    private NoopTrafficAccountant() {}

    public static TrafficAccountant getInstance() {
        return INSTANCE;
    }

    @Override
    public void notifyRead(long bytes) {}

    @Override
    public void notifyWrite(long bytes) {}

    @Override
    public void tryCheck() {}
}
