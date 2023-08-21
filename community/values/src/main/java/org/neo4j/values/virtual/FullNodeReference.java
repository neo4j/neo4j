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
import org.neo4j.values.ElementIdMapper;

public class FullNodeReference extends VirtualNodeReference {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(FullNodeReference.class);

    private final long id;
    private final ElementIdMapper elementIdMapper;
    private String elementId;

    FullNodeReference(long id, String elementId) {
        assert elementId != null;
        this.id = id;
        this.elementId = elementId;
        this.elementIdMapper = null;
    }

    FullNodeReference(long id, ElementIdMapper elementIdMapper) {
        assert elementIdMapper != null;
        this.id = id;
        this.elementId = null;
        this.elementIdMapper = elementIdMapper;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        writer.writeNodeReference(id);
    }

    @Override
    public String getTypeName() {
        return "FullNodeReference";
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
    public String elementId() {
        if (elementId == null) {
            elementId = elementIdMapper.nodeElementId(id);
        }
        return elementId;
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE;
    }
}
