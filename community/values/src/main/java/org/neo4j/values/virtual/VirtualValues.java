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

import static org.neo4j.memory.HeapEstimator.sizeOf;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.virtual.PathValue.DirectPathValue;

/**
 * Entry point to the virtual values library.
 */
public final class VirtualValues {
    public static final MapValue EMPTY_MAP = MapValue.EMPTY;
    public static final ListValue EMPTY_LIST =
            new ListValue.ArrayListValue(new AnyValue[0], 0, ValueRepresentation.ANYTHING);

    private VirtualValues() {}

    // DIRECT FACTORY METHODS

    public static ListValue list(AnyValue... values) {
        long payloadSize = 0;
        ValueRepresentation representation = ValueRepresentation.ANYTHING;
        for (AnyValue value : values) {
            payloadSize += value.estimatedHeapUsage();
            if (value.valueRepresentation() != representation) {
                representation = representation.coerce(value.valueRepresentation());
            }
        }
        return new ListValue.ArrayListValue(values, payloadSize, representation);
    }

    public static ListValue fromList(List<AnyValue> values) {
        long payloadSize = 0;
        ValueRepresentation representation = ValueRepresentation.ANYTHING;
        for (AnyValue value : values) {
            payloadSize += value.estimatedHeapUsage();
            representation = representation.coerce(value.valueRepresentation());
        }
        return new ListValue.JavaListListValue(values, payloadSize, representation);
    }

    public static ListValue range(long start, long end, long step) {
        return new ListValue.IntegralRangeListValue(start, end, step);
    }

    public static ListValue.ArrayValueListValue fromArray(ArrayValue arrayValue) {
        return new ListValue.ArrayValueListValue(arrayValue);
    }

    /*
    TOMBSTONE: TransformedListValue & FilteredListValue

    This list value variant would lazily apply a transform/filter on a inner list. The lazy behavior made it hard
    to guarantee that the transform/filter was still evaluable and correct on reading the transformed list, so
    this was removed. If we want lazy values again, remember the problems of

       - returning results out of Cypher combined with auto-closing iterators
       - reading modified tx-state which was not visible at TransformedListValue creation

    */

    public static ListValue concat(ListValue... lists) {
        return new ListValue.ConcatList(lists);
    }

    public static MapValue map(String[] keys, AnyValue[] values) {
        assert keys.length == values.length;
        long payloadSize = 0;
        Map<String, AnyValue> map = new HashMap<>((int) ((float) keys.length / 0.75f + 1.0f));
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            AnyValue value = values[i];
            map.put(key, value);
            payloadSize += sizeOf(key) + value.estimatedHeapUsage();
        }
        return new MapValue.MapWrappingMapValue(map, payloadSize);
    }

    public static MapValue fromMap(Map<String, AnyValue> map, long mapSize, long payloadSize) {
        return new MapValue.MapWrappingMapValue(map, mapSize, payloadSize);
    }

    public static ErrorValue error(Exception e) {
        return new ErrorValue(e);
    }

    public static NodeIdReference node(long id) {
        return new NodeIdReference(id);
    }

    public static FullNodeReference node(long id, ElementIdMapper mapper) {
        return new FullNodeReference(id, mapper);
    }

    public static FullNodeReference node(long id, String elementId) {
        return new FullNodeReference(id, elementId);
    }

    public static FullNodeReference node(long id, String elementId, long sourceId) {
        return new CompositeDatabaseValue.CompositeFullNodeReference(id, elementId, sourceId);
    }

    public static RelationshipReference relationship(long id) {
        return new RelationshipReference(id);
    }

    public static RelationshipReference relationship(long id, long startNode, long endNode) {
        return new RelationshipReference(id, startNode, endNode);
    }

    public static RelationshipReference relationship(long id, long startNode, long endNode, int type) {
        return new RelationshipReference(id, startNode, endNode, type);
    }

    public static PathReference pathReference(long[] nodes, long[] relationships) {
        assert nodes != null;
        assert relationships != null;
        if ((nodes.length + relationships.length) % 2 == 0) {
            throw new IllegalArgumentException(
                    "Tried to construct a path that is not built like a path: even number of elements");
        }
        assert nodes.length == relationships.length + 1;

        return PathReference.path(nodes, relationships);
    }

    public static PathReference pathReference(VirtualNodeValue[] nodes, VirtualRelationshipValue[] relationships) {
        assert nodes != null;
        assert relationships != null;
        if ((nodes.length + relationships.length) % 2 == 0) {
            throw new IllegalArgumentException(
                    "Tried to construct a path that is not built like a path: even number of elements");
        }
        assert nodes.length == relationships.length + 1;

        return PathReference.path(nodes, relationships);
    }

    public static PathValue path(NodeValue[] nodes, RelationshipValue[] relationships) {
        assert nodes != null;
        assert relationships != null;
        if ((nodes.length + relationships.length) % 2 == 0) {
            throw new IllegalArgumentException(
                    "Tried to construct a path that is not built like a path: even number of elements");
        }
        long payloadSize = 0;
        assert nodes.length == relationships.length + 1;
        int i = 0;
        for (; i < relationships.length; i++) {
            payloadSize += nodes[i].estimatedHeapUsage() + relationships[i].estimatedHeapUsage();
        }
        payloadSize += nodes[i].estimatedHeapUsage();

        return new DirectPathValue(nodes, relationships, payloadSize);
    }

    public static PathValue path(NodeValue[] nodes, RelationshipValue[] relationships, long payloadSize) {
        assert nodes != null;
        assert relationships != null;
        if ((nodes.length + relationships.length) % 2 == 0) {
            throw new IllegalArgumentException(
                    "Tried to construct a path that is not built like a path: even number of elements");
        }
        return new DirectPathValue(nodes, relationships, payloadSize);
    }

    public static NodeValue nodeValue(long id, String elementId, TextArray labels, MapValue properties) {
        return new NodeValue.DirectNodeValue(id, elementId, labels, properties, false);
    }

    public static NodeValue compositeGraphNodeValue(
            long id, String elementId, long sourceId, TextArray labels, MapValue properties) {
        return new CompositeDatabaseValue.CompositeGraphDirectNodeValue(
                id, elementId, sourceId, labels, properties, false);
    }

    public static NodeValue nodeValue(
            long id, String elementId, TextArray labels, MapValue properties, boolean isDeleted) {
        return new NodeValue.DirectNodeValue(id, elementId, labels, properties, isDeleted);
    }

    public static RelationshipValue relationshipValue(
            long id,
            String elementId,
            VirtualNodeReference startNode,
            VirtualNodeReference endNode,
            TextValue type,
            MapValue properties) {
        return new RelationshipValue.DirectRelationshipValue(
                id, elementId, startNode, endNode, type, properties, false);
    }

    public static RelationshipValue compositeGraphRelationshipValue(
            long id,
            String elementId,
            long sourceId,
            VirtualNodeReference startNode,
            VirtualNodeReference endNode,
            TextValue type,
            MapValue properties) {
        return new CompositeDatabaseValue.CompositeDirectRelationshipValue(
                id, elementId, sourceId, startNode, endNode, type, properties, false);
    }

    public static RelationshipValue relationshipValue(
            long id,
            String elementId,
            VirtualNodeReference startNode,
            VirtualNodeReference endNode,
            TextValue type,
            MapValue properties,
            boolean isDeleted) {
        return new RelationshipValue.DirectRelationshipValue(
                id, elementId, startNode, endNode, type, properties, isDeleted);
    }

    public static ListValue asList(AnyValue collection) {
        if (collection == NO_VALUE) {
            return VirtualValues.EMPTY_LIST;
        } else if (collection instanceof ListValue) {
            return (ListValue) collection;
        } else if (collection instanceof ArrayValue) {
            return VirtualValues.fromArray((ArrayValue) collection);
        } else {
            return VirtualValues.list(collection);
        }
    }
}
