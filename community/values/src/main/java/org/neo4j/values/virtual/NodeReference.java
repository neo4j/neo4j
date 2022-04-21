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
package org.neo4j.values.virtual;

import static java.lang.String.format;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.ElementIdMapper;

public class NodeReference extends VirtualNodeValue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(NodeReference.class);

    private final long id;
    private final String elementId;
    private final ElementIdMapper elementIdMapper;

    NodeReference(long id) {
        this(id, null, null);
    }

    NodeReference(long id, String elementId, ElementIdMapper elementIdMapper) {
        this.id = id;
        this.elementId = elementId;
        this.elementIdMapper = elementIdMapper;
    }

    @Override
    public String elementId() {
        if (elementId != null) {
            return elementId;
        }
        if (elementIdMapper == null) {
            throw new UnsupportedOperationException(
                    "This is tricky to implement for NodeReference because of the disconnected nature of it. "
                            + "Didn't we want to get rid of this thing completely?");
        }
        return elementIdMapper.nodeElementId(id);
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        writer.writeNodeReference(id);
    }

    @Override
    public String getTypeName() {
        return "NodeReference";
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

    @Override
    ElementIdMapper elementIdMapper() {
        return elementIdMapper;
    }
}
