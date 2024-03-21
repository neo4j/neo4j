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
package org.neo4j.kernel.impl.util;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.exceptions.StoreFailureException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.VirtualValues;

public class NodeEntityWrappingNodeValue extends NodeValue implements WrappingEntity<Node> {
    static final long SHALLOW_SIZE = shallowSizeOfInstance(NodeEntityWrappingNodeValue.class) + NodeEntity.SHALLOW_SIZE;

    private final Node node;
    private volatile TextArray labels;
    private volatile MapValue properties;

    NodeEntityWrappingNodeValue(Node node) {
        super(node.getId());
        this.node = node;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        if (writer.entityMode() == REFERENCE) {
            writer.writeNodeReference(id());
        } else {
            TextArray l;
            MapValue p;
            boolean isDeleted = false;
            try {
                l = labels();
                p = properties();
            } catch (ReadAndDeleteTransactionConflictException e) {
                if (!e.wasDeletedInThisTransaction()) {
                    throw e;
                }
                // If it isn't a transient error then the node was deleted in the current transaction and we should
                // write an 'empty' node.
                l = Values.stringArray();
                p = VirtualValues.EMPTY_MAP;
                isDeleted = true;
            }

            if (id() < 0) {
                writer.writeVirtualNodeHack(node);
            }

            writer.writeNode(node.getElementId(), node.getId(), l, p, isDeleted);
        }
    }

    public void populate(NodeCursor nodeCursor, PropertyCursor propertyCursor) {
        try {
            labels(nodeCursor);
            properties(nodeCursor, propertyCursor);
        } catch (ReadAndDeleteTransactionConflictException e) {
            if (!e.wasDeletedInThisTransaction()) {
                throw e;
            }
            // best effort, cannot do more
        }
    }

    public boolean isPopulated() {
        return labels != null && properties != null;
    }

    public boolean canPopulate() {
        if (node instanceof NodeEntity entity) {
            return entity.getTransaction().isOpen();
        }
        return true;
    }

    public TextArray labels(NodeCursor nodeCursor) {
        TextArray l = labels;
        if (l == null) {
            try {
                synchronized (this) {
                    l = labels;
                    if (l == null) {
                        List<String> ls = new ArrayList<>();
                        // No DBHits for Virtual node hacks.
                        var nodeLabels = node instanceof NodeEntity
                                ? ((NodeEntity) node).getLabels(nodeCursor)
                                : node.getLabels();
                        for (Label label : nodeLabels) {
                            ls.add(label.name());
                        }
                        l = labels = Values.stringArray(ls.toArray(new String[0]));
                    }
                }
            } catch (NotFoundException | IllegalStateException | StoreFailureException e) {
                throw new ReadAndDeleteTransactionConflictException(NodeEntity.isDeletedInCurrentTransaction(node), e);
            }
        }
        return l;
    }

    @Override
    public TextArray labels() {
        TextArray l = labels;
        if (l == null) {
            try {
                synchronized (this) {
                    l = labels;
                    if (l == null) {
                        List<String> ls = new ArrayList<>();
                        for (Label label : node.getLabels()) {
                            ls.add(label.name());
                        }
                        l = labels = Values.stringArray(ls.toArray(new String[0]));
                    }
                }
            } catch (NotFoundException | IllegalStateException | StoreFailureException e) {
                throw new ReadAndDeleteTransactionConflictException(NodeEntity.isDeletedInCurrentTransaction(node), e);
            }
        }
        return l;
    }

    @Override
    public MapValue properties() {
        MapValue m = properties;
        if (m == null) {
            try {
                synchronized (this) {
                    m = properties;
                    if (m == null) {
                        m = properties = ValueUtils.asMapValue(node.getAllProperties());
                    }
                }
            } catch (NotFoundException | IllegalStateException | StoreFailureException e) {
                throw new ReadAndDeleteTransactionConflictException(NodeEntity.isDeletedInCurrentTransaction(node), e);
            }
        }
        return m;
    }

    @Override
    public String elementId() {
        return node.getElementId();
    }

    public MapValue properties(NodeCursor nodeCursor, PropertyCursor propertyCursor) {
        MapValue m = properties;
        if (m == null) {
            try {
                synchronized (this) {
                    m = properties;
                    if (m == null) {
                        // No DBHits for Virtual node hacks.
                        var nodeProperties = node instanceof NodeEntity
                                ? ((NodeEntity) node).getAllProperties(nodeCursor, propertyCursor)
                                : node.getAllProperties();
                        m = properties = ValueUtils.asMapValue(nodeProperties);
                    }
                }
            } catch (NotFoundException | IllegalStateException | StoreFailureException e) {
                throw new ReadAndDeleteTransactionConflictException(NodeEntity.isDeletedInCurrentTransaction(node), e);
            }
        }
        return m;
    }

    @Override
    public long estimatedHeapUsage() {
        long size = SHALLOW_SIZE;
        if (labels != null) {
            size += labels.estimatedHeapUsage();
        }
        if (properties != null) {
            size += properties.estimatedHeapUsage();
        }
        return size;
    }

    @Override
    public Node getEntity() {
        return node;
    }
}
