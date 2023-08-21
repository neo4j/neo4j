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
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextArray;

public abstract class NodeValue extends VirtualNodeReference {
    private final long id;

    protected NodeValue(long id) {
        this.id = id;
    }

    public abstract TextArray labels();

    public abstract MapValue properties();

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        if (writer.entityMode() == REFERENCE) {
            writer.writeNodeReference(id);
        } else {
            writer.writeNode(elementId(), id, labels(), properties(), isDeleted());
        }
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public String toString() {
        return format("(%d)", id);
    }

    @Override
    public String getTypeName() {
        return "Node";
    }

    private static final long DIRECT_NODE_SHALLOW_SIZE = shallowSizeOfInstance(DirectNodeValue.class);

    public static class DirectNodeValue extends NodeValue {
        private final String elementId;
        private final TextArray labels;
        private final MapValue properties;
        private final boolean isDeleted;

        /**
         * @param id internal id of the node.
         * @param labels label names of this node.
         * @param properties properties of this node.
         * @param isDeleted whether this node is deleted.
         */
        DirectNodeValue(long id, String elementId, TextArray labels, MapValue properties, boolean isDeleted) {
            super(id);
            assert elementId != null;
            assert labels != null;
            assert properties != null;
            this.elementId = elementId;
            this.labels = labels;
            this.properties = properties;
            this.isDeleted = isDeleted;
        }

        @Override
        public TextArray labels() {
            return labels;
        }

        @Override
        public MapValue properties() {
            return properties;
        }

        @Override
        public long estimatedHeapUsage() {
            return DIRECT_NODE_SHALLOW_SIZE + labels.estimatedHeapUsage() + properties.estimatedHeapUsage();
        }

        @Override
        public boolean isDeleted() {
            return isDeleted;
        }

        @Override
        public String elementId() {
            return elementId;
        }
    }
}
