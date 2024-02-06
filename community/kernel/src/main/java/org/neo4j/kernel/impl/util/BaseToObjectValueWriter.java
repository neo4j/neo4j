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

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.internal.helpers.collection.ReverseArrayIterator;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

/**
 * Base class for converting AnyValue to normal java objects.
 * <p>
 * This base class takes care of converting all "normal" java types such as
 * number types, booleans, strings, arrays and lists. It leaves to the extending
 * class to handle neo4j specific types such as nodes, edges and points.
 *
 * @param <E> the exception thrown on error.
 */
public abstract class BaseToObjectValueWriter<E extends Exception> implements AnyValueWriter<E> {
    private final Deque<Writer> stack = new ArrayDeque<>();

    public BaseToObjectValueWriter() {
        stack.push(new ObjectWriter());
    }

    protected abstract Node newNodeEntityById(long id);

    protected abstract Node newNodeEntityByElementId(String elementId);

    protected abstract Relationship newRelationshipEntityById(long id);

    protected abstract Relationship newRelationshipEntityByElementId(String elementId);

    protected abstract Point newPoint(CoordinateReferenceSystem crs, double[] coordinate);

    public Object value() {
        assert stack.size() == 1;
        return stack.getLast().value();
    }

    private void writeValue(Object value) {
        assert !stack.isEmpty();
        Writer head = stack.peek();
        head.write(value);
    }

    @Override
    public EntityMode entityMode() {
        return EntityMode.FULL;
    }

    @Override
    public void writeNodeReference(long nodeId) {
        throw new UnsupportedOperationException("Cannot write a raw node reference");
    }

    @Override
    public void writeNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted)
            throws E {
        if (nodeId >= 0) {
            writeValue(newNodeEntityByElementId(elementId));
        }
    }

    @Override
    public void writeVirtualNodeHack(Object node) {
        writeValue(node);
    }

    @Override
    public void writeRelationshipReference(long relId) {
        throw new UnsupportedOperationException("Cannot write a raw relationship reference");
    }

    @Override
    public void writeRelationship(
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted)
            throws E {
        if (relId >= 0) {
            writeValue(newRelationshipEntityByElementId(elementId));
        }
    }

    @Override
    public void writeVirtualRelationshipHack(Object relationship) {
        writeValue(relationship);
    }

    @Override
    public void beginMap(int size) {
        stack.push(new MapWriter(size));
    }

    @Override
    public void endMap() {
        assert !stack.isEmpty();
        writeValue(stack.pop().value());
    }

    @Override
    public void beginList(int size) {
        stack.push(new ListWriter(size));
    }

    @Override
    public void endList() {
        assert !stack.isEmpty();
        writeValue(stack.pop().value());
    }

    @Override
    public void writePathReference(long[] nodes, long[] relationships) {
        assert nodes != null;
        assert nodes.length > 0;
        assert relationships != null;
        assert nodes.length == relationships.length + 1;

        Node[] nodeProxies = new Node[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            nodeProxies[i] = newNodeEntityById(nodes[i]);
        }
        Relationship[] relProxies = new Relationship[relationships.length];
        for (int i = 0; i < relationships.length; i++) {
            relProxies[i] = newRelationshipEntityById(relationships[i]);
        }
        writeValue(new PathProxy(nodeProxies, relProxies));
    }

    @Override
    public void writePathReference(VirtualNodeValue[] nodes, VirtualRelationshipValue[] relationships) {
        assert nodes != null;
        assert nodes.length > 0;
        assert relationships != null;
        assert nodes.length == relationships.length + 1;

        Node[] nodeProxies = new Node[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            nodeProxies[i] = newNodeEntityById(nodes[i].id());
        }
        Relationship[] relProxies = new Relationship[relationships.length];
        for (int i = 0; i < relationships.length; i++) {
            relProxies[i] = newRelationshipEntityById(relationships[i].id());
        }
        writeValue(new PathProxy(nodeProxies, relProxies));
    }

    @Override
    public void writePathReference(List<VirtualNodeValue> nodes, List<VirtualRelationshipValue> relationships)
            throws E {
        assert nodes != null;
        assert nodes.size() > 0;
        assert relationships != null;
        assert nodes.size() == relationships.size() + 1;

        Node[] nodeProxies = new Node[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            nodeProxies[i] = newNodeEntityById(nodes.get(i).id());
        }
        Relationship[] relProxies = new Relationship[relationships.size()];
        for (int i = 0; i < relationships.size(); i++) {
            relProxies[i] = newRelationshipEntityById(relationships.get(i).id());
        }
        writeValue(new PathProxy(nodeProxies, relProxies));
    }

    @Override
    public void writePath(NodeValue[] nodes, RelationshipValue[] relationships) {
        assert nodes != null;
        assert nodes.length > 0;
        assert relationships != null;
        assert nodes.length == relationships.length + 1;

        Node[] nodeProxies = new Node[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            nodeProxies[i] = newNodeEntityById(nodes[i].id());
        }
        Relationship[] relProxies = new Relationship[relationships.length];
        for (int i = 0; i < relationships.length; i++) {
            relProxies[i] = newRelationshipEntityById(relationships[i].id());
        }
        writeValue(new PathProxy(nodeProxies, relProxies));
    }

    @Override
    public final void writePoint(CoordinateReferenceSystem crs, double[] coordinate) {
        writeValue(newPoint(crs, coordinate));
    }

    @Override
    public void writeNull() {
        writeValue(null);
    }

    @Override
    public void writeBoolean(boolean value) {
        writeValue(value);
    }

    @Override
    public void writeInteger(byte value) {
        writeValue(value);
    }

    @Override
    public void writeInteger(short value) {
        writeValue(value);
    }

    @Override
    public void writeInteger(int value) {
        writeValue(value);
    }

    @Override
    public void writeInteger(long value) {
        writeValue(value);
    }

    @Override
    public void writeFloatingPoint(float value) {
        writeValue(value);
    }

    @Override
    public void writeFloatingPoint(double value) {
        writeValue(value);
    }

    @Override
    public void writeString(String value) {
        writeValue(value);
    }

    @Override
    public void writeString(char value) {
        writeValue(value);
    }

    @Override
    public void beginArray(int size, ArrayType arrayType) {
        stack.push(new ArrayWriter(size, arrayType));
    }

    @Override
    public void endArray() {
        assert !stack.isEmpty();
        writeValue(stack.pop().value());
    }

    @Override
    public void writeByteArray(byte[] value) {
        writeValue(value);
    }

    @Override
    public void writeDuration(long months, long days, long seconds, int nanos) {
        writeValue(DurationValue.duration(months, days, seconds, nanos));
    }

    @Override
    public void writeDate(LocalDate localDate) {
        writeValue(localDate);
    }

    @Override
    public void writeLocalTime(LocalTime localTime) {
        writeValue(localTime);
    }

    @Override
    public void writeTime(OffsetTime offsetTime) {
        writeValue(offsetTime);
    }

    @Override
    public void writeLocalDateTime(LocalDateTime localDateTime) {
        writeValue(localDateTime);
    }

    @Override
    public void writeDateTime(ZonedDateTime zonedDateTime) {
        writeValue(zonedDateTime);
    }

    private static class PathProxy implements Path {
        private final Node[] nodes;
        private final Relationship[] relationships;

        private PathProxy(Node[] nodes, Relationship[] relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }

        @Override
        public Node startNode() {
            return nodes[0];
        }

        @Override
        public Node endNode() {
            return nodes[nodes.length - 1];
        }

        @Override
        public Relationship lastRelationship() {
            return relationships[relationships.length - 1];
        }

        @Override
        public Iterable<Relationship> relationships() {
            return Arrays.asList(relationships);
        }

        @Override
        public Iterable<Relationship> reverseRelationships() {
            return () -> new ReverseArrayIterator<>(relationships);
        }

        @Override
        public Iterable<Node> nodes() {
            return Arrays.asList(nodes);
        }

        @Override
        public Iterable<Node> reverseNodes() {
            return () -> new ReverseArrayIterator<>(nodes);
        }

        @Override
        public int length() {
            return relationships.length;
        }

        @Override
        public int hashCode() {
            if (relationships.length == 0) {
                return startNode().hashCode();
            } else {
                return Arrays.hashCode(relationships);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj instanceof Path other) {
                return startNode().equals(other.startNode())
                        && iteratorsEqual(
                                this.relationships().iterator(),
                                other.relationships().iterator());
            } else {
                return false;
            }
        }

        @Override
        public Iterator<Entity> iterator() {
            return new Iterator<>() {
                Iterator<? extends Entity> current = nodes().iterator();
                Iterator<? extends Entity> next = relationships().iterator();

                @Override
                public boolean hasNext() {
                    return current.hasNext();
                }

                @Override
                public Entity next() {
                    try {
                        return current.next();
                    } finally {
                        Iterator<? extends Entity> temp = current;
                        current = next;
                        next = temp;
                    }
                }

                @Override
                public void remove() {
                    next.remove();
                }
            };
        }

        @Override
        public String toString() {
            return Paths.defaultPathToStringWithNotInTransactionFallback(this);
        }
    }

    private interface Writer {
        void write(Object value);

        Object value();
    }

    private static class ObjectWriter implements Writer {
        private Object value;

        @Override
        public void write(Object value) {
            this.value = value;
        }

        @Override
        public Object value() {
            return value;
        }
    }

    private static class MapWriter implements Writer {
        private String key;
        private boolean isKey = true;
        private final Map<String, Object> map;

        MapWriter(int size) {
            this.map = new HashMap<>(size);
        }

        @Override
        public void write(Object value) {
            if (isKey) {
                key = (String) value;
                isKey = false;
            } else {
                map.put(key, value);
                isKey = true;
            }
        }

        @Override
        public Object value() {
            return map;
        }
    }

    private static class ArrayWriter implements Writer {
        protected final Object array;
        private int index;

        ArrayWriter(int size, ArrayType arrayType) {
            switch (arrayType) {
                case SHORT:
                    this.array = Array.newInstance(short.class, size);
                    break;
                case INT:
                    this.array = Array.newInstance(int.class, size);
                    break;
                case BYTE:
                    this.array = Array.newInstance(byte.class, size);
                    break;
                case LONG:
                    this.array = Array.newInstance(long.class, size);
                    break;
                case FLOAT:
                    this.array = Array.newInstance(float.class, size);
                    break;
                case DOUBLE:
                    this.array = Array.newInstance(double.class, size);
                    break;
                case BOOLEAN:
                    this.array = Array.newInstance(boolean.class, size);
                    break;
                case STRING:
                    this.array = Array.newInstance(String.class, size);
                    break;
                case CHAR:
                    this.array = Array.newInstance(char.class, size);
                    break;
                default:
                    this.array = new Object[size];
            }
        }

        @Override
        public void write(Object value) {
            Array.set(array, index++, value);
        }

        @Override
        public Object value() {
            return array;
        }
    }

    private static class ListWriter implements Writer {
        private final List<Object> list;

        ListWriter(int size) {
            this.list = new ArrayList<>(size);
        }

        @Override
        public void write(Object value) {
            list.add(value);
        }

        @Override
        public Object value() {
            return list;
        }
    }
}
