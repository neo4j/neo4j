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

import java.util.function.Consumer;
import org.neo4j.values.AnyValueWriter;

public class RelationshipReference extends VirtualRelationshipValue implements RelationshipVisitor {
    static final long NO_NODE = -1L;
    static final int NO_TYPE = -1;
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(RelationshipReference.class);

    private final long id;
    private long startNode;
    private long endNode;
    private int type;

    RelationshipReference(long id) {
        this(id, NO_NODE, NO_NODE, NO_TYPE);
    }

    RelationshipReference(long id, long startNode, long endNode) {
        this(id, startNode, endNode, NO_TYPE);
    }

    RelationshipReference(long id, long startNode, long endNode, int type) {
        this.id = id;
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
    }

    @Override
    public long startNodeId(Consumer<RelationshipVisitor> consumer) {
        if (startNode == NO_NODE) {
            consumer.accept(this);
        }
        return startNode;
    }

    @Override
    public long endNodeId(Consumer<RelationshipVisitor> consumer) {
        if (endNode == NO_NODE) {
            consumer.accept(this);
        }
        return endNode;
    }

    @Override
    public int relationshipTypeId(Consumer<RelationshipVisitor> consumer) {
        if (type == NO_TYPE) {
            consumer.accept(this);
        }
        return type;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        writer.writeRelationshipReference(id);
    }

    @Override
    public String getTypeName() {
        return "RelationshipReference";
    }

    @Override
    public String toString() {
        return format("-[%d]-", id);
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
    public void visit(long startNode, long endNode, int type) {
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
    }
}
