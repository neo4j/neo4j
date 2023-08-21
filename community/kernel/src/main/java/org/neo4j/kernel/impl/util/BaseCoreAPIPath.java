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

import static org.neo4j.internal.helpers.collection.Iterators.iteratorsEqual;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.values.virtual.VirtualPathValue;

public abstract class BaseCoreAPIPath implements Path {
    private final VirtualPathValue value;

    protected BaseCoreAPIPath(VirtualPathValue value) {
        this.value = value;
    }

    public VirtualPathValue pathValue() {
        return value;
    }

    @Override
    public String toString() {
        return Paths.defaultPathToStringWithNotInTransactionFallback(this);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof BaseCoreAPIPath) {
            return value.equals(((BaseCoreAPIPath) obj).value);
        } else if (obj instanceof Path other) {
            if (value.nodeIds()[0] != other.startNode().getId()) {
                return false;
            }
            return iteratorsEqual(
                    this.relationships().iterator(), other.relationships().iterator());
        } else {
            return false;
        }
    }

    @Override
    public Node startNode() {
        return mapNode(value.nodeIds()[0]);
    }

    @Override
    public Node endNode() {
        long[] nodeIds = value.nodeIds();
        return mapNode(nodeIds[nodeIds.length - 1]);
    }

    @Override
    public Relationship lastRelationship() {
        if (value.size() == 0) {
            return null;
        } else {
            long[] relationshipIds = value.relationshipIds();
            return mapRelationship(relationshipIds[relationshipIds.length - 1]);
        }
    }

    @Override
    public Iterable<Relationship> relationships() {
        return asList(value.relationshipIds(), this::mapRelationship);
    }

    @Override
    public Iterable<Relationship> reverseRelationships() {
        return asReverseList(value.relationshipIds(), this::mapRelationship);
    }

    @Override
    public Iterable<Node> nodes() {
        return asList(value.nodeIds(), this::mapNode);
    }

    @Override
    public Iterable<Node> reverseNodes() {
        return asReverseList(value.nodeIds(), this::mapNode);
    }

    @Override
    public int length() {
        return value.size();
    }

    @Override
    public Iterator<Entity> iterator() {
        return new Iterator<>() {
            private final int size = 2 * value.size() + 1;
            private int index;
            private final long[] nodes = value.nodeIds();
            private final long[] relationships = value.relationshipIds();

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public Entity next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Entity entity;
                if ((index & 1) == 0) {
                    entity = mapNode(nodes[index >> 1]);
                } else {
                    entity = mapRelationship(relationships[index >> 1]);
                }
                index++;
                return entity;
            }
        };
    }

    private static <V> Iterable<V> asList(long[] values, LongToObjectFunction<V> mapper) {
        return () -> new Iterator<>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < values.length;
            }

            @Override
            public V next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return mapper.apply(values[index++]);
            }
        };
    }

    private static <V> Iterable<V> asReverseList(long[] values, LongToObjectFunction<V> mapper) {
        return () -> new Iterator<>() {
            private int index = values.length - 1;

            @Override
            public boolean hasNext() {
                return index >= 0;
            }

            @Override
            public V next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return mapper.apply(values[index--]);
            }
        };
    }

    protected abstract Node mapNode(long value);

    protected abstract Relationship mapRelationship(long value);
}
