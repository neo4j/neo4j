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
package org.neo4j.values.virtual;

import static java.lang.String.format;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import org.neo4j.values.AnyValueWriter;

/**
 * Reference to a node by its long id, does not contain element id as a runtime optimisation.
 */
public class NodeIdReference extends VirtualNodeValue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(NodeIdReference.class);

    private final long id;

    NodeIdReference(long id) {
        this.id = id;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        writer.writeNodeReference(id);
    }

    @Override
    public String getTypeName() {
        return "NodeIdReference";
    }

    @Override
    public String toString() {
        return format("(%d)", id);
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE;
    }
}
