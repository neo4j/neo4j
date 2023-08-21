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
package org.neo4j.storageengine.api.format;

/**
 * A special capability used for marking formats
 * that are compatible with 4.4 indexing.
 */
public class Index44Compatibility implements Capability {
    public static final Index44Compatibility INSTANCE = new Index44Compatibility();

    private Index44Compatibility() {}

    @Override
    public boolean isType(CapabilityType type) {
        // This capability has no type - to not trigger any format compatibility checks and do extra unnecessary
        // migration
        // It is just used as a marker to know if format has 4.4 indexing and should treat schema store special
        return false;
    }

    @Override
    public boolean isAdditive() {
        return false;
    }
}
