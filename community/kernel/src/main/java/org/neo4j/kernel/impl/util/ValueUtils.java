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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.eclipse.collections.api.multimap.Multimap;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextNode;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextRelationship;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextValueMapper;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualNodeReference;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

public final class ValueUtils {
    private ValueUtils() {
        throw new UnsupportedOperationException("do not instantiate");
    }

    public static AnyValue of(Object object) {
        return of(object, false);
    }

    /**
     * Creates an AnyValue by doing type inspection. Do not use in production code where performance is important.
     *
     * @param object the object to turned into a AnyValue
     * @return the AnyValue corresponding to object.
     */
    @SuppressWarnings("unchecked")
    public static AnyValue of(Object object, boolean wrapEntities) {
        if (object instanceof AnyValue) {
            return (AnyValue) object;
        }
        Value value = Values.unsafeOf(object, true);
        if (value != null) {
            return value;
        } else {
            if (object instanceof Entity) {
                if (object instanceof Node) {
                    if (wrapEntities) {
                        return maybeWrapNodeEntity((Node) object);
                    } else {
                        return fromNodeEntity((Node) object);
                    }
                } else if (object instanceof Relationship) {
                    if (wrapEntities) {
                        return maybeWrapRelationshipEntity((Relationship) object);
                    } else {
                        return fromRelationshipEntity((Relationship) object);
                    }
                } else {
                    throw new IllegalArgumentException(
                            "Unknown entity + " + object.getClass().getName());
                }
            } else if (object instanceof Multimap<?, ?>) {
                return asMultimapValue((Multimap<String, Object>) object, wrapEntities);
            } else if (object instanceof Map<?, ?>) {
                return asMapValue((Map<String, Object>) object, wrapEntities);
            } else if (object instanceof Iterable<?>) {
                if (object instanceof Path) {
                    if (wrapEntities) {
                        return maybeWrapPath((Path) object);
                    } else {
                        return pathReferenceFromPath((Path) object);
                    }
                } else if (object instanceof List<?>) {
                    return asListValue((List<Object>) object, wrapEntities);
                } else {
                    return asListValue((Iterable<Object>) object, wrapEntities);
                }
            } else if (object instanceof Iterator<?> iterator) {
                ListValueBuilder builder = ListValueBuilder.newListBuilder();
                while (iterator.hasNext()) {
                    builder.add(ValueUtils.of(iterator.next(), wrapEntities));
                }
                return builder.build();
            } else if (object instanceof Object[] array) {
                if (array.length == 0) {
                    return VirtualValues.EMPTY_LIST;
                }

                ListValueBuilder builder = ListValueBuilder.newListBuilder(array.length);
                for (Object o : array) {
                    builder.add(ValueUtils.of(o, wrapEntities));
                }
                return builder.build();
            } else if (object instanceof Stream<?>) {
                return asListValue(((Stream<Object>) object).collect(Collectors.toList()));
            } else if (object instanceof Geometry) {
                return asGeometryValue((Geometry) object);
            } else {
                ClassLoader classLoader = object.getClass().getClassLoader();
                throw new IllegalArgumentException(String.format(
                        "Cannot convert %s of type %s to AnyValue, classloader=%s, classloader-name=%s",
                        object,
                        object.getClass().getName(),
                        classLoader != null ? classLoader.toString() : "null",
                        classLoader != null ? classLoader.getName() : "null"));
            }
        }
    }

    public static PointValue asPointValue(Point point) {
        return toPoint(point);
    }

    public static PointValue asGeometryValue(Geometry geometry) {
        if (!geometry.getGeometryType().equals("Point")) {
            throw new IllegalArgumentException(
                    "Cannot handle geometry type: " + geometry.getCRS().getType());
        }
        return toPoint(geometry);
    }

    private static PointValue toPoint(Geometry geometry) {
        double[] coordinatesCopy = geometry.getCoordinates().get(0).getCoordinateCopy();
        return Values.pointValue(CoordinateReferenceSystem.get(geometry.getCRS()), coordinatesCopy);
    }

    public static ListValue asListValue(List<?> collection) {
        return asListValue(collection, false);
    }

    public static ListValue asListValue(List<?> collection, boolean wrapEntities) {
        int size = collection.size();
        if (size == 0) {
            return VirtualValues.EMPTY_LIST;
        }

        ListValueBuilder values = ListValueBuilder.newListBuilder(size);
        for (Object o : collection) {
            values.add(ValueUtils.of(o, wrapEntities));
        }
        return values.build();
    }

    public static ListValue asListValue(Iterable<?> collection) {
        return asListValue(collection, false);
    }

    public static ListValue asListValue(Iterable<?> collection, boolean wrapEntities) {
        ListValueBuilder values = ListValueBuilder.newListBuilder();
        for (Object o : collection) {
            values.add(ValueUtils.of(o, wrapEntities));
        }
        return values.build();
    }

    public static AnyValue asNodeOrEdgeValue(Entity container) {
        if (container instanceof Node) {
            return fromNodeEntity((Node) container);
        } else if (container instanceof Relationship) {
            return fromRelationshipEntity((Relationship) container);
        } else {
            throw new IllegalArgumentException(
                    "Cannot produce a node or edge from " + container.getClass().getName());
        }
    }

    public static ListValue asListOfEdges(Iterable<Relationship> rels) {
        return VirtualValues.list(StreamSupport.stream(rels.spliterator(), false)
                .map(ValueUtils::fromRelationshipEntity)
                .toArray(VirtualRelationshipValue[]::new));
    }

    public static ListValue asListOfEdges(Relationship[] rels) {
        if (rels.length == 0) {
            return VirtualValues.EMPTY_LIST;
        }

        ListValueBuilder relValues = ListValueBuilder.newListBuilder(rels.length);
        for (Relationship rel : rels) {
            relValues.add(fromRelationshipEntity(rel));
        }
        return relValues.build();
    }

    public static MapValue asMapValue(Map<String, ?> map) {
        return asMapValue(map, false);
    }

    public static MapValue asMapValue(Map<String, ?> map, boolean wrapEntities) {
        int size = map.size();
        if (size == 0) {
            return VirtualValues.EMPTY_MAP;
        }

        MapValueBuilder builder = new MapValueBuilder(size);
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            builder.add(entry.getKey(), ValueUtils.of(entry.getValue(), wrapEntities));
        }
        return builder.build();
    }

    public static MapValue asMultimapValue(Multimap<String, ?> map) {
        return asMultimapValue(map, false);
    }

    public static MapValue asMultimapValue(Multimap<String, ?> map, boolean wrapEntities) {
        if (map.isEmpty()) {
            return VirtualValues.EMPTY_MAP;
        }

        MapValueBuilder builder = new MapValueBuilder(map.sizeDistinct());
        map.forEachKeyMultiValues((key, values) -> {
            builder.add(key, of(values, wrapEntities));
        });
        return builder.build();
    }

    public static MapValue asParameterMapValue(Map<String, Object> map) {
        int size = map.size();
        if (size == 0) {
            return VirtualValues.EMPTY_MAP;
        }

        MapValueBuilder builder = new MapValueBuilder(size);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            try {
                builder.add(entry.getKey(), ValueUtils.of(entry.getValue(), true));
            } catch (IllegalArgumentException e) {
                builder.add(entry.getKey(), VirtualValues.error(e));
            }
        }

        return builder.build();
    }

    public static VirtualNodeValue fromNodeEntity(Node node) {
        // sigh: negative ids are used as a mechanism for transferring "fake" entities from and to procedures.
        // We use it ourselves in some internal procedures, see VirtualNodeHack, and it is also used extensively
        // in apoc.
        return asNodeReference(node);
    }

    public static VirtualNodeReference asNodeReference(Node node) {
        // sigh, see above
        if (node.getId() < 0) {
            return new NodeEntityWrappingNodeValue(node);
        } else {
            return VirtualValues.node(node.getId(), node.getElementId());
        }
    }

    // sigh: For procedures we must support "fake" entities from and to procedures.
    // We use it ourselves in some internal procedures, see VirtualNodeHack, and it is also used extensively
    // in apoc.
    @Deprecated
    public static VirtualNodeValue wrapNodeEntity(Node node) {
        return new NodeEntityWrappingNodeValue(node);
    }

    @Deprecated
    public static VirtualNodeValue maybeWrapNodeEntity(Node node) {
        // Execution Context Node is a special implementation of Node used in procedures and functions invoked
        // from parallel runtime. It contains a reference to an Execution Context. Execution Context is always
        // owned by one thread, therefore references to Execution Contexts must not escape to be possibly
        // used concurrently by other threads. Execution Context Node must therefore always be turned into
        // a reference type.
        if (node instanceof ExecutionContextNode) {
            return fromNodeEntity(node);
        } else {
            return wrapNodeEntity(node);
        }
    }

    public static VirtualRelationshipValue fromRelationshipEntity(Relationship relationship) {
        // sigh: negative ids are used as a mechanism for transferring "fake" entities from and to procedures.
        // We use it ourselves in some internal procedures, see VirtualNodeHack, and it is also used extensively
        // in apoc.
        if (relationship.getId() < 0) {
            return wrapRelationshipEntity(relationship);
        } else {
            return VirtualValues.relationship(relationship.getId());
        }
    }

    // sigh: For procedures we must support "fake" entities from and to procedures.
    // We use it ourselves in some internal procedures, see VirtualNodeHack, and it is also used extensively
    // in apoc.
    @Deprecated
    public static VirtualRelationshipValue wrapRelationshipEntity(Relationship relationship) {
        return RelationshipEntityWrappingValue.wrapLazy(relationship);
    }

    @Deprecated
    public static VirtualRelationshipValue maybeWrapRelationshipEntity(Relationship relationship) {
        // Execution Context Relationship is a special implementation of Relationship used in procedures and functions
        // invoked from parallel runtime. It contains a reference to an Execution Context. Execution Context is always
        // owned by one thread, therefore references to Execution Contexts must not escape to be possibly
        // used concurrently by other threads. Execution Context Relationship must therefore always be turned into
        // a reference type.
        if (relationship instanceof ExecutionContextRelationship) {
            return fromRelationshipEntity(relationship);
        } else {
            return wrapRelationshipEntity(relationship);
        }
    }

    public static VirtualPathValue fromPath(Path path) {
        return VirtualValues.pathReference(
                StreamSupport.stream(path.nodes().spliterator(), false)
                        .map(ValueUtils::fromNodeEntity)
                        .toList(),
                StreamSupport.stream(path.relationships().spliterator(), false)
                        .map(ValueUtils::fromRelationshipEntity)
                        .toList());
    }

    // sigh: For procedures we must support "fake" entities from and to procedures.
    // We use it ourselves in some internal procedures, see VirtualNodeHack, and it is also used extensively
    // in apoc.
    @Deprecated
    public static VirtualPathValue wrapPath(Path path) {
        return new PathWrappingPathValue(path);
    }

    @Deprecated
    public static VirtualPathValue maybeWrapPath(Path path) {
        // Execution Context Path is a special implementation of Path used in procedures and functions
        // invoked from parallel runtime. It contains a reference to an Execution Context. Execution Context is always
        // owned by one thread, therefore references to Execution Contexts must not escape to be possibly
        // used concurrently by other threads. Execution Context Path must therefore always be turned into
        // a reference type.
        if (path instanceof ExecutionContextValueMapper.ExecutionContextPath) {
            return pathReferenceFromPath(path);
        } else {
            return new PathWrappingPathValue(path);
        }
    }

    public static VirtualPathValue pathReferenceFromPath(Path path) {
        if (path instanceof BaseCoreAPIPath) {
            return ((BaseCoreAPIPath) path).pathValue();
        } else {
            int len = path.length();
            long[] nodes = new long[len + 1];
            long[] rels = new long[len];
            Iterator<Node> nodeIterator = path.nodes().iterator();
            Iterator<Relationship> relIterator = path.relationships().iterator();
            int i = 0;
            for (; i < len; i++) {
                nodes[i] = nodeIterator.next().getId();
                rels[i] = relIterator.next().getId();
            }
            nodes[i] = nodeIterator.next().getId();
            return VirtualValues.pathReference(nodes, rels);
        }
    }

    /**
     * Creates a {@link Value} from the given object, or if it is already a Value it is returned as it is.
     * <p>
     * This is different from {@link Values#of} which often explicitly fails or creates a new copy
     * if given a Value.
     */
    public static Value asValue(Object value) {
        if (value instanceof Value) {
            return (Value) value;
        }
        return Values.of(value);
    }

    /**
     * Creates an {@link AnyValue} from the given object, or if it is already an AnyValue it is returned as it is.
     * <p>
     * This is different from {@link ValueUtils#of} which often explicitly fails or creates a new copy
     * if given an AnyValue.
     */
    public static AnyValue asAnyValue(Object value) {
        if (value instanceof AnyValue) {
            return (AnyValue) value;
        }
        return ValueUtils.of(value);
    }

    @CalledFromGeneratedCode
    public static VirtualNodeValue asNodeValue(Object object) {
        if (object instanceof VirtualNodeValue) {
            return (VirtualNodeValue) object;
        }
        if (object instanceof Node) {
            return fromNodeEntity((Node) object);
        }
        throw new IllegalArgumentException(
                "Cannot produce a node from " + object.getClass().getName());
    }

    @CalledFromGeneratedCode
    public static VirtualRelationshipValue asRelationshipValue(Object object) {
        if (object instanceof VirtualRelationshipValue) {
            return (VirtualRelationshipValue) object;
        } else if (object instanceof Relationship) {
            return fromRelationshipEntity((Relationship) object);
        } else {
            throw new IllegalArgumentException(
                    "Cannot produce a relationship from " + object.getClass().getName());
        }
    }

    @CalledFromGeneratedCode
    public static LongValue asLongValue(Object object) {
        if (object instanceof LongValue) {
            return (LongValue) object;
        }
        if (object instanceof Long) {
            return Values.longValue((long) object);
        }
        if (object instanceof IntValue) {
            return Values.longValue(((IntValue) object).longValue());
        }
        if (object instanceof Integer) {
            return Values.longValue((int) object);
        }

        throw new IllegalArgumentException(
                "Cannot produce a long from " + object.getClass().getName());
    }

    @CalledFromGeneratedCode
    public static DoubleValue asDoubleValue(Object object) {
        if (object instanceof DoubleValue) {
            return (DoubleValue) object;
        }
        if (object instanceof Double) {
            return Values.doubleValue((double) object);
        }
        throw new IllegalArgumentException(
                "Cannot produce a double from " + object.getClass().getName());
    }

    @CalledFromGeneratedCode
    public static BooleanValue asBooleanValue(Object object) {
        if (object instanceof BooleanValue) {
            return (BooleanValue) object;
        }
        if (object instanceof Boolean) {
            return Values.booleanValue((boolean) object);
        }
        throw new IllegalArgumentException(
                "Cannot produce a boolean from " + object.getClass().getName());
    }

    @CalledFromGeneratedCode
    public static TextValue asTextValue(Object object) {
        if (object instanceof TextValue) {
            return (TextValue) object;
        }
        if (object instanceof String) {
            return Values.utf8Value((String) object);
        }
        throw new IllegalArgumentException(
                "Cannot produce a string from " + object.getClass().getName());
    }
}
