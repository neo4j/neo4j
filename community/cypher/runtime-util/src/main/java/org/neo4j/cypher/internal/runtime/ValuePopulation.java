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
package org.neo4j.cypher.internal.runtime;

import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.EMPTY_TEXT_ARRAY;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue;
import org.neo4j.kernel.impl.util.ReadAndDeleteTransactionConflictException;
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.HeapTrackingListValueBuilder;
import org.neo4j.values.virtual.HeapTrackingMapValueBuilder;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

public final class ValuePopulation {
    private static final NodeValue MISSING_NODE = VirtualValues.nodeValue(-1L, "", EMPTY_TEXT_ARRAY, EMPTY_MAP, false);

    private ValuePopulation() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    /**
     * Populates nodes and relationships contained in the specified value.
     *
     * Note about memory tracking!
     * Population can potentially allocate lots of memory, for example large lists of node references.
     * To try to avoid some OOMs, we sometimes(!) allocate on the provided memory tracker in these methods.
     * However, because we don't have ownership of the resources, they are also released from memory tracking
     * before returning. This provides some safety for really large populations, but is also flawed because the
     * instances will obviously live on after memory is released from the tracker.
     * <p>
     * At the time of writing, runtime have no memory tracking of data that is in-flight in the operator.
     * If that is added at some point, memory tracking in these methods should be revisited to avoid over-estimation.
     * For example in queries like `WITH [1,2,3] AS x RETURN x`, if the memory of x is already tracked by the operator
     * it will be allocated twice when reaching here. Some queries are less straight forward, like `MATCH (n) RETURN n`.
     * If the operator keeps track of in-flight memory it needs to consider that value population will allocate more
     * memory.
     *
     */
    public static AnyValue populate(
            AnyValue value,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            MemoryTracker memoryTracker) {
        if (value instanceof VirtualNodeValue node) {
            return populate(node, dbAccess, nodeCursor, propertyCursor);
        } else if (value instanceof VirtualRelationshipValue relationship) {
            return populate(relationship, dbAccess, relCursor, propertyCursor);
        } else if (value instanceof VirtualPathValue path) {
            return populate(path, dbAccess, nodeCursor, relCursor, propertyCursor);
        } else if (value instanceof ListValue list && needsPopulation(list)) {
            return populate(list, dbAccess, nodeCursor, relCursor, propertyCursor, memoryTracker);
        } else if (value instanceof MapValue map) {
            return populate(map, dbAccess, nodeCursor, relCursor, propertyCursor, memoryTracker);
        } else {
            return value;
        }
    }

    private static boolean needsPopulation(final ListValue list) {
        final var itemType = list.itemValueRepresentation();
        return itemType == ValueRepresentation.UNKNOWN || itemType == ValueRepresentation.ANYTHING;
    }

    public static NodeValue populate(
            VirtualNodeValue value, DbAccess dbAccess, NodeCursor nodeCursor, PropertyCursor propertyCursor) {
        if (value instanceof NodeEntityWrappingNodeValue wrappingNodeValue && !wrappingNodeValue.isPopulated()) {
            if (wrappingNodeValue.canPopulate()) {
                wrappingNodeValue.populate(nodeCursor, propertyCursor);
                return wrappingNodeValue;
            } else {
                // Node was created in an inner transaction that has been closed
                return nodeValue(value.id(), dbAccess, nodeCursor, propertyCursor);
            }
        } else if (value instanceof NodeValue) {
            return (NodeValue) value;
        } else {
            return nodeValue(value.id(), dbAccess, nodeCursor, propertyCursor);
        }
    }

    public static RelationshipValue populate(
            VirtualRelationshipValue value,
            DbAccess dbAccess,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor) {
        if (value instanceof RelationshipEntityWrappingValue wrappingValue && !wrappingValue.isPopulated()) {
            if (wrappingValue.canPopulate()) {
                wrappingValue.populate(relCursor, propertyCursor);
                return wrappingValue;
            } else {
                // Relationship was created in an inner transaction that has been closed
                return relationshipValue(value.id(), dbAccess, relCursor, propertyCursor);
            }
        } else if (value instanceof RelationshipValue) {
            return (RelationshipValue) value;
        } else {
            return relationshipValue(value.id(), dbAccess, relCursor, propertyCursor);
        }
    }

    private static PathValue populate(
            VirtualPathValue value,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor) {
        if (value instanceof PathValue) {
            return (PathValue) value;
        } else {
            var nodeIds = value.nodeIds();
            var relIds = value.relationshipIds();
            var nodes = new NodeValue[nodeIds.length];
            var rels = new RelationshipValue[relIds.length];
            long payloadSize = 0;
            // we know that rels.length + 1 = nodes.length
            int i = 0;
            for (; i < rels.length; i++) {
                NodeValue nodeValue = nodeValue(nodeIds[i], dbAccess, nodeCursor, propertyCursor);
                RelationshipValue relationshipValue = relationshipValue(relIds[i], dbAccess, relCursor, propertyCursor);
                payloadSize += nodeValue.estimatedHeapUsage() + relationshipValue.estimatedHeapUsage();
                nodes[i] = nodeValue;
                rels[i] = relationshipValue;
            }
            NodeValue nodeValue = nodeValue(nodeIds[i], dbAccess, nodeCursor, propertyCursor);
            payloadSize += nodeValue.estimatedHeapUsage();
            nodes[i] = nodeValue;

            return VirtualValues.path(nodes, rels, payloadSize);
        }
    }

    private static MapValue populate(
            MapValue value,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            MemoryTracker memoryTracker) {
        final var builder = HeapTrackingMapValueBuilder.newHeapTrackingMapValueBuilder(memoryTracker);

        value.foreach((key, anyValue) ->
                builder.put(key, populate(anyValue, dbAccess, nodeCursor, relCursor, propertyCursor, memoryTracker)));

        // Values are still in memory but harder to track after this point.
        // The intention is to at least avoid OOM during population of heavy maps.
        return builder.buildAndClose();
    }

    private static ListValue populate(
            ListValue value,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            MemoryTracker memoryTracker) {
        final var builder = new HeapTrackingListValueBuilder(memoryTracker);
        for (AnyValue v : value) {
            builder.add(populate(v, dbAccess, nodeCursor, relCursor, propertyCursor, memoryTracker));
        }
        return builder.buildAndClose();
    }

    private static NodeValue nodeValue(
            long id, DbAccess dbAccess, NodeCursor nodeCursor, PropertyCursor propertyCursor) {
        dbAccess.singleNode(id, nodeCursor);
        final var elementId = dbAccess.elementIdMapper().nodeElementId(id);

        if (!nodeCursor.next()) {
            if (!dbAccess.nodeDeletedInThisTransaction(id)) {
                throw new ReadAndDeleteTransactionConflictException(false);
            } else {
                return VirtualValues.nodeValue(id, elementId, EMPTY_TEXT_ARRAY, EMPTY_MAP, true);
            }
        } else {
            nodeCursor.properties(propertyCursor);
            return VirtualValues.nodeValue(
                    id, elementId, labels(dbAccess, nodeCursor.labels()), properties(propertyCursor, dbAccess));
        }
    }

    private static RelationshipValue relationshipValue(
            long id, DbAccess dbAccess, RelationshipScanCursor relCursor, PropertyCursor propertyCursor) {
        dbAccess.singleRelationship(id, relCursor);
        final var idMapper = dbAccess.elementIdMapper();
        final var elementId = idMapper.relationshipElementId(id);

        if (!relCursor.next()) {
            if (!dbAccess.relationshipDeletedInThisTransaction(id)) {
                throw new ReadAndDeleteTransactionConflictException(false);
            } else {
                return VirtualValues.relationshipValue(
                        id, elementId, MISSING_NODE, MISSING_NODE, EMPTY_STRING, EMPTY_MAP, true);
            }
        } else {
            // Bolt doesn't require start and end node to be populated
            final var start = VirtualValues.node(relCursor.sourceNodeReference(), idMapper);
            final var end = VirtualValues.node(relCursor.targetNodeReference(), idMapper);
            relCursor.properties(propertyCursor);
            return VirtualValues.relationshipValue(
                    id,
                    elementId,
                    start,
                    end,
                    Values.stringValue(dbAccess.relationshipTypeName(relCursor.type())),
                    properties(propertyCursor, dbAccess));
        }
    }

    private static TextArray labels(DbAccess dbAccess, TokenSet labelsTokens) {
        String[] labels = new String[labelsTokens.numberOfTokens()];
        for (int i = 0; i < labels.length; i++) {
            labels[i] = dbAccess.nodeLabelName(labelsTokens.token(i));
        }
        return Values.stringArray(labels);
    }

    private static MapValue properties(PropertyCursor propertyCursor, DbAccess dbAccess) {
        MapValueBuilder builder = new MapValueBuilder();
        while (propertyCursor.next()) {
            builder.add(dbAccess.propertyKeyName(propertyCursor.propertyKey()), propertyCursor.propertyValue());
        }
        return builder.build();
    }
}
