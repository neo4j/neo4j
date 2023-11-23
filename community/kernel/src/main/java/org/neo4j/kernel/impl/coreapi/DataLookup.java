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
package org.neo4j.kernel.impl.coreapi;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.filter;
import static org.neo4j.internal.helpers.collection.Iterators.firstOrDefault;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
import static org.neo4j.kernel.impl.newapi.CursorPredicates.nodeMatchProperties;
import static org.neo4j.kernel.impl.newapi.CursorPredicates.relationshipMatchProperties;
import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.values.storable.Values.utf8Value;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.internal.kernel.api.CloseListener;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.coreapi.internal.NodeLabelPropertyIterator;
import org.neo4j.kernel.impl.coreapi.internal.RelationshipTypePropertyIterator;
import org.neo4j.kernel.impl.coreapi.internal.TrackedCursorIterator;
import org.neo4j.kernel.impl.newapi.CursorPredicates;
import org.neo4j.kernel.impl.newapi.FilteringNodeCursorWrapper;
import org.neo4j.kernel.impl.newapi.FilteringRelationshipScanCursorWrapper;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.Values;

public abstract class DataLookup {

    public Node getNodeById(long id) {
        if (id < 0 || !dataRead().nodeExists(id)) {
            throw new NotFoundException(
                    format("Node %d not found", id), new EntityNotFoundException(EntityType.NODE, valueOf(id)));
        }

        return newNodeEntity(id);
    }

    public Node getNodeByElementId(String elementId) {
        long nodeId = elementIdMapper().nodeId(elementId);
        if (!dataRead().nodeExists(nodeId)) {
            throw new NotFoundException(
                    format("Node %s not found.", elementId), new EntityNotFoundException(EntityType.NODE, elementId));
        }
        return newNodeEntity(nodeId);
    }

    public ResourceIterator<Node> findNodes(Label myLabel) {
        checkLabel(myLabel);
        return allNodesWithLabel(myLabel);
    }

    public ResourceIterator<Node> findNodes(Label myLabel, String key, String value, StringSearchMode searchMode) {
        checkLabel(myLabel);
        checkPropertyKey(key);
        checkArgument(value != null, "Template must not be null");
        TokenRead tokenRead = tokenRead();
        int labelId = tokenRead.nodeLabel(myLabel.name());
        int propertyId = tokenRead.propertyKey(key);
        if (invalidTokens(labelId, propertyId)) {
            return emptyResourceIterator();
        }
        PropertyIndexQuery query = getIndexQuery(value, searchMode, propertyId);
        IndexDescriptor index =
                findUsableMatchingIndex(SchemaDescriptors.forLabel(labelId, propertyId), IndexType.TEXT, query);

        // We didn't find an index, but we might be able to used RANGE and filtering - let's see
        if (index == IndexDescriptor.NO_INDEX
                && (searchMode == StringSearchMode.SUFFIX || searchMode == StringSearchMode.CONTAINS)) {
            PropertyIndexQuery.RangePredicate<?> allStringQuery =
                    PropertyIndexQuery.range(propertyId, (String) null, false, null, false);
            index = findUsableMatchingIndex(SchemaDescriptors.forLabel(labelId, propertyId), allStringQuery);
            if (index != IndexDescriptor.NO_INDEX && index.getCapability().supportsReturningValues()) {
                return nodesByLabelAndPropertyWithFiltering(labelId, allStringQuery, index, query);
            }
        }

        return nodesByLabelAndProperty(labelId, query, index);
    }

    public Node findNode(Label myLabel, String key, Object value) {
        try (ResourceIterator<Node> iterator = findNodes(myLabel, key, value)) {
            if (!iterator.hasNext()) {
                return null;
            }
            Node node = iterator.next();
            if (iterator.hasNext()) {
                throw new MultipleFoundException(format(
                        "Found multiple nodes with label: '%s', property name: '%s' and property "
                                + "value: '%s' while only one was expected.",
                        myLabel, key, value));
            }
            return node;
        }
    }

    public ResourceIterator<Node> findNodes(Label myLabel, String key, Object value) {
        checkLabel(myLabel);
        checkPropertyKey(key);
        TokenRead tokenRead = tokenRead();
        int labelId = tokenRead.nodeLabel(myLabel.name());
        int propertyId = tokenRead.propertyKey(key);
        if (invalidTokens(labelId, propertyId)) {
            return emptyResourceIterator();
        }
        PropertyIndexQuery.ExactPredicate query = PropertyIndexQuery.exact(propertyId, Values.of(value, false));
        IndexDescriptor index = findUsableMatchingIndex(SchemaDescriptors.forLabel(labelId, propertyId), query);
        return nodesByLabelAndProperty(labelId, query, index);
    }

    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2) {
        checkLabel(label);
        checkPropertyKey(key1);
        checkPropertyKey(key2);
        TokenRead tokenRead = tokenRead();
        int labelId = tokenRead.nodeLabel(label.name());
        return nodesByLabelAndProperties(
                labelId,
                PropertyIndexQuery.exact(tokenRead.propertyKey(key1), Values.of(value1, false)),
                PropertyIndexQuery.exact(tokenRead.propertyKey(key2), Values.of(value2, false)));
    }

    public ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues) {
        checkLabel(label);
        checkArgument(propertyValues != null, "Property values can not be null");
        TokenRead tokenRead = tokenRead();
        int labelId = tokenRead.nodeLabel(label.name());
        PropertyIndexQuery.ExactPredicate[] queries = convertToQueries(propertyValues, tokenRead);
        return nodesByLabelAndProperties(labelId, queries);
    }

    public ResourceIterator<Node> findNodes(
            Label label, String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        checkLabel(label);
        checkPropertyKey(key1);
        checkPropertyKey(key2);
        checkPropertyKey(key3);
        TokenRead tokenRead = tokenRead();
        int labelId = tokenRead.nodeLabel(label.name());
        return nodesByLabelAndProperties(
                labelId,
                PropertyIndexQuery.exact(tokenRead.propertyKey(key1), Values.of(value1, false)),
                PropertyIndexQuery.exact(tokenRead.propertyKey(key2), Values.of(value2, false)),
                PropertyIndexQuery.exact(tokenRead.propertyKey(key3), Values.of(value3, false)));
    }

    public Relationship getRelationshipById(long id) {
        if (id < 0) {
            throw new NotFoundException(
                    format("Relationship with %d not found", id),
                    new EntityNotFoundException(EntityType.RELATIONSHIP, valueOf(id)));
        }

        if (!dataRead().relationshipExists(id)) {
            throw new NotFoundException(
                    format("Relationship with %d not found", id),
                    new EntityNotFoundException(EntityType.RELATIONSHIP, valueOf(id)));
        }
        return newRelationshipEntity(id);
    }

    public Relationship getRelationshipByElementId(String elementId) {
        long relationshipId = elementIdMapper().relationshipId(elementId);
        if (!dataRead().relationshipExists(relationshipId)) {
            throw new NotFoundException(
                    format("Relationship %s not found.", elementId),
                    new EntityNotFoundException(EntityType.RELATIONSHIP, elementId));
        }
        return newRelationshipEntity(relationshipId);
    }

    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key, String template, StringSearchMode searchMode) {
        checkRelationshipType(relationshipType);
        checkPropertyKey(key);
        checkArgument(template != null, "Template must not be null");
        TokenRead tokenRead = tokenRead();
        int typeId = tokenRead.relationshipType(relationshipType.name());
        int propertyId = tokenRead.propertyKey(key);
        if (invalidTokens(typeId, propertyId)) {
            return emptyResourceIterator();
        }
        PropertyIndexQuery query = getIndexQuery(template, searchMode, propertyId);
        IndexDescriptor index =
                findUsableMatchingIndex(SchemaDescriptors.forRelType(typeId, propertyId), IndexType.TEXT, query);

        // We didn't find an index, but we might be able to used RANGE and filtering - let's see
        if (index == IndexDescriptor.NO_INDEX
                && (searchMode == StringSearchMode.SUFFIX || searchMode == StringSearchMode.CONTAINS)) {
            PropertyIndexQuery.RangePredicate<?> allStringQuery =
                    PropertyIndexQuery.range(propertyId, (String) null, false, null, false);
            index = findUsableMatchingIndex(SchemaDescriptors.forRelType(typeId, propertyId), allStringQuery);
            if (index != IndexDescriptor.NO_INDEX && index.getCapability().supportsReturningValues()) {
                return relationshipsByTypeAndPropertyWithFiltering(typeId, allStringQuery, index, query);
            }
        }

        return relationshipsByTypeAndProperty(typeId, query, index);
    }

    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, Map<String, Object> propertyValues) {
        checkRelationshipType(relationshipType);
        checkArgument(propertyValues != null, "Property values can not be null");
        TokenRead tokenRead = tokenRead();
        int typeId = tokenRead.relationshipType(relationshipType.name());
        PropertyIndexQuery.ExactPredicate[] queries = convertToQueries(propertyValues, tokenRead);
        return relationshipsByTypeAndProperties(typeId, queries);
    }

    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType,
            String key1,
            Object value1,
            String key2,
            Object value2,
            String key3,
            Object value3) {
        checkRelationshipType(relationshipType);
        checkPropertyKey(key1);
        checkPropertyKey(key2);
        checkPropertyKey(key3);
        TokenRead tokenRead = tokenRead();
        int typeId = tokenRead.relationshipType(relationshipType.name());
        return relationshipsByTypeAndProperties(
                typeId,
                PropertyIndexQuery.exact(tokenRead.propertyKey(key1), Values.of(value1, false)),
                PropertyIndexQuery.exact(tokenRead.propertyKey(key2), Values.of(value2, false)),
                PropertyIndexQuery.exact(tokenRead.propertyKey(key3), Values.of(value3, false)));
    }

    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key1, Object value1, String key2, Object value2) {
        checkRelationshipType(relationshipType);
        checkPropertyKey(key1);
        checkPropertyKey(key2);
        TokenRead tokenRead = tokenRead();
        int typeId = tokenRead.relationshipType(relationshipType.name());
        return relationshipsByTypeAndProperties(
                typeId,
                PropertyIndexQuery.exact(tokenRead.propertyKey(key1), Values.of(value1, false)),
                PropertyIndexQuery.exact(tokenRead.propertyKey(key2), Values.of(value2, false)));
    }

    public Relationship findRelationship(RelationshipType relationshipType, String key, Object value) {
        try (var iterator = findRelationships(relationshipType, key, value)) {
            if (!iterator.hasNext()) {
                return null;
            }
            var rel = iterator.next();
            if (iterator.hasNext()) {
                throw new MultipleFoundException(format(
                        "Found multiple relationships with type: '%s', property name: '%s' and property "
                                + "value: '%s' while only one was expected.",
                        relationshipType, key, value));
            }
            return rel;
        }
    }

    public ResourceIterator<Relationship> findRelationships(
            RelationshipType relationshipType, String key, Object value) {
        checkRelationshipType(relationshipType);
        checkPropertyKey(key);
        TokenRead tokenRead = tokenRead();
        int typeId = tokenRead.relationshipType(relationshipType.name());
        int propertyId = tokenRead.propertyKey(key);
        if (invalidTokens(typeId, propertyId)) {
            return emptyResourceIterator();
        }
        PropertyIndexQuery.ExactPredicate query = PropertyIndexQuery.exact(propertyId, Values.of(value, false));
        IndexDescriptor index = findUsableMatchingIndex(SchemaDescriptors.forRelType(typeId, propertyId), query);
        return relationshipsByTypeAndProperty(typeId, query, index);
    }

    public ResourceIterator<Relationship> findRelationships(RelationshipType relationshipType) {
        checkRelationshipType(relationshipType);
        return allRelationshipsWithType(relationshipType);
    }

    public Iterable<Label> getAllLabelsInUse() {
        performCheckBeforeOperation();
        return allInUse(TokenAccess.LABELS);
    }

    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        performCheckBeforeOperation();
        return allInUse(TokenAccess.RELATIONSHIP_TYPES);
    }

    public Iterable<Label> getAllLabels() {
        performCheckBeforeOperation();
        return all(TokenAccess.LABELS);
    }

    public Iterable<RelationshipType> getAllRelationshipTypes() {
        performCheckBeforeOperation();
        return all(TokenAccess.RELATIONSHIP_TYPES);
    }

    public Iterable<String> getAllPropertyKeys() {
        performCheckBeforeOperation();
        return all(TokenAccess.PROPERTY_KEYS);
    }

    private ResourceIterator<Relationship> allRelationshipsWithType(final RelationshipType type) {
        int typeId = tokenRead().relationshipType(type.name());
        if (typeId == TokenRead.NO_TOKEN) {
            return emptyResourceIterator();
        }

        TokenPredicate query = new TokenPredicate(typeId);
        var index = findUsableMatchingIndex(ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR, query);

        if (index != IndexDescriptor.NO_INDEX) {
            try {
                var session = dataRead().tokenReadSession(index);
                var cursor = cursors().allocateRelationshipTypeIndexCursor(cursorContext(), memoryTracker());
                dataRead().relationshipTypeScan(session, cursor, unconstrained(), query, cursorContext());
                return new TrackedCursorIterator<>(
                        cursor,
                        RelationshipIndexCursor::relationshipReference,
                        c -> newRelationshipEntity(c.relationshipReference()),
                        resourceMonitor());
            } catch (KernelException e) {
                // ignore, fallback to all node scan
            }
        }

        return allRelationshipsByTypeWithoutIndex(typeId);
    }

    private ResourceIterator<Relationship> allRelationshipsByTypeWithoutIndex(int typeId) {
        var cursor = cursors().allocateRelationshipScanCursor(cursorContext(), memoryTracker());
        dataRead().allRelationshipsScan(cursor);
        var filteredCursor = new FilteringRelationshipScanCursorWrapper(cursor, CursorPredicates.hasType(typeId));
        return new TrackedCursorIterator<>(
                filteredCursor,
                RelationshipScanCursor::relationshipReference,
                c -> newRelationshipEntity(
                        c.relationshipReference(), c.sourceNodeReference(), c.type(), c.targetNodeReference()),
                resourceMonitor());
    }

    private ResourceIterator<Node> allNodesWithLabel(Label myLabel) {
        int labelId = tokenRead().nodeLabel(myLabel.name());
        if (labelId == TokenRead.NO_TOKEN) {
            return emptyResourceIterator();
        }

        TokenPredicate query = new TokenPredicate(labelId);
        var index = findUsableMatchingIndex(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, query);

        if (index != IndexDescriptor.NO_INDEX) {
            try {
                var session = dataRead().tokenReadSession(index);
                var cursor = cursors().allocateNodeLabelIndexCursor(cursorContext(), memoryTracker());
                dataRead().nodeLabelScan(session, cursor, unconstrained(), query, cursorContext());
                return new TrackedCursorIterator<>(
                        cursor,
                        NodeIndexCursor::nodeReference,
                        c -> newNodeEntity(c.nodeReference()),
                        resourceMonitor());
            } catch (KernelException e) {
                // ignore, fallback to all node scan
            }
        }

        return allNodesByLabelWithoutIndex(labelId);
    }

    private ResourceIterator<Node> allNodesByLabelWithoutIndex(int labelId) {
        NodeCursor cursor = cursors().allocateNodeCursor(cursorContext(), memoryTracker());
        dataRead().allNodesScan(cursor);
        var filteredCursor = new FilteringNodeCursorWrapper(cursor, CursorPredicates.hasLabel(labelId));
        return new TrackedCursorIterator<>(
                filteredCursor, NodeCursor::nodeReference, c -> newNodeEntity(c.nodeReference()), resourceMonitor());
    }

    private IndexDescriptor findUsableMatchingIndex(
            SchemaDescriptor schemaDescriptor, IndexType preference, IndexQuery... query) {
        List<IndexDescriptor> indexes = asList(getMatchingOnlineIndexes(schemaDescriptor, query));
        Optional<IndexDescriptor> preferred = indexes.stream()
                .filter(index -> index.getIndexType() == preference)
                .findAny();
        return preferred.orElse(firstOrDefault(indexes.iterator(), IndexDescriptor.NO_INDEX));
    }

    /**
     * Find an ONLINE index that matches the schema.
     */
    private IndexDescriptor findUsableMatchingIndex(SchemaDescriptor schemaDescriptor, IndexQuery... query) {
        return firstOrDefault(getMatchingOnlineIndexes(schemaDescriptor, query), IndexDescriptor.NO_INDEX);
    }

    private Iterator<IndexDescriptor> getMatchingOnlineIndexes(SchemaDescriptor schemaDescriptor, IndexQuery... query) {
        SchemaRead schemaRead = schemaRead();
        Iterator<IndexDescriptor> iterator = schemaRead.index(schemaDescriptor);
        return filter(index -> indexIsOnline(schemaRead, index) && indexSupportQuery(index, query), iterator);
    }

    private ResourceIterator<Node> nodesByLabelAndPropertyWithFiltering(
            int labelId, PropertyIndexQuery query, IndexDescriptor index, PropertyIndexQuery originalQuery) {
        Read read = dataRead();
        try {
            NodeValueIndexCursor cursor = cursors().allocateNodeValueIndexCursor(cursorContext(), memoryTracker());
            IndexReadSession indexSession = read.indexReadSession(index);
            read.nodeIndexSeek(queryContext(), indexSession, cursor, unorderedValues(), query);

            return new TrackedCursorIterator<>(
                    new FilteringCursor<>(cursor, originalQuery),
                    c -> cursor.nodeReference(),
                    c -> newNodeEntity(cursor.nodeReference()),
                    resourceMonitor());
        } catch (KernelException e) {
            // weird at this point but ignore and fallback to a label scan
        }

        return getNodesByLabelAndPropertyWithoutPropertyIndex(labelId, query);
    }

    private ResourceIterator<Node> getNodesByLabelAndPropertyWithoutPropertyIndex(
            int labelId, PropertyIndexQuery... queries) {
        TokenPredicate tokenQuery = new TokenPredicate(labelId);
        var index = findUsableMatchingIndex(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, tokenQuery);

        if (index != IndexDescriptor.NO_INDEX) {
            try {
                var session = dataRead().tokenReadSession(index);
                var cursor = cursors().allocateNodeLabelIndexCursor(cursorContext(), memoryTracker());
                dataRead().nodeLabelScan(session, cursor, unconstrained(), tokenQuery, cursorContext());

                var nodeCursor = cursors().allocateNodeCursor(cursorContext(), memoryTracker());
                var propertyCursor = cursors().allocatePropertyCursor(cursorContext(), memoryTracker());

                return new NodeLabelPropertyIterator(
                        dataRead(),
                        cursor,
                        nodeCursor,
                        propertyCursor,
                        c -> newNodeEntity(c.nodeReference()),
                        resourceMonitor(),
                        queries);
            } catch (KernelException e) {
                // ignore, fallback to all node scan
            }
        }
        return getNodesByLabelAndPropertyViaAllNodesScan(labelId, queries);
    }

    private TrackedCursorIterator<FilteringNodeCursorWrapper, Node> getNodesByLabelAndPropertyViaAllNodesScan(
            int labelId, PropertyIndexQuery[] queries) {
        var nodeCursor = cursors().allocateNodeCursor(cursorContext(), memoryTracker());
        var labelFilteredCursor = new FilteringNodeCursorWrapper(nodeCursor, CursorPredicates.hasLabel(labelId));

        var propertyCursor = cursors().allocatePropertyCursor(cursorContext(), memoryTracker());
        var propertyFilteredCursor = new FilteringNodeCursorWrapper(
                labelFilteredCursor, nodeMatchProperties(queries, propertyCursor), List.of(propertyCursor));

        dataRead().allNodesScan(nodeCursor);
        return new TrackedCursorIterator<>(
                propertyFilteredCursor,
                NodeCursor::nodeReference,
                c -> newNodeEntity(c.nodeReference()),
                resourceMonitor());
    }

    private ResourceIterator<Node> nodesByLabelAndProperty(
            int labelId, PropertyIndexQuery query, IndexDescriptor index) {
        Read read = dataRead();
        if (index != IndexDescriptor.NO_INDEX) {
            // Ha! We found an index - let's use it to find matching nodes
            try {
                NodeValueIndexCursor cursor = cursors().allocateNodeValueIndexCursor(cursorContext(), memoryTracker());
                IndexReadSession indexSession = read.indexReadSession(index);
                read.nodeIndexSeek(queryContext(), indexSession, cursor, unconstrained(), query);

                return new TrackedCursorIterator<>(
                        cursor,
                        NodeIndexCursor::nodeReference,
                        c -> newNodeEntity(c.nodeReference()),
                        resourceMonitor());
            } catch (KernelException e) {
                // weird at this point but ignore and fallback to a label scan
            }
        }

        return getNodesByLabelAndPropertyWithoutPropertyIndex(labelId, query);
    }

    private ResourceIterator<Node> nodesByLabelAndProperties(
            int labelId, PropertyIndexQuery.ExactPredicate... queries) {
        Read read = dataRead();

        if (isInvalidQuery(labelId, queries)) {
            return emptyResourceIterator();
        }

        int[] propertyIds = getPropertyIds(queries);
        IndexDescriptor index = findUsableMatchingCompositeIndex(
                SchemaDescriptors.forLabel(labelId, propertyIds),
                propertyIds,
                () -> schemaRead().indexesGetForLabel(labelId),
                queries);

        if (index != IndexDescriptor.NO_INDEX) {
            try {
                NodeValueIndexCursor cursor = cursors().allocateNodeValueIndexCursor(cursorContext(), memoryTracker());
                IndexReadSession indexSession = read.indexReadSession(index);
                read.nodeIndexSeek(
                        queryContext(),
                        indexSession,
                        cursor,
                        unconstrained(),
                        getReorderedIndexQueries(index.schema().getPropertyIds(), queries));
                return new TrackedCursorIterator<>(
                        cursor,
                        NodeIndexCursor::nodeReference,
                        c -> newNodeEntity(c.nodeReference()),
                        resourceMonitor());
            } catch (KernelException e) {
                // weird at this point but ignore and fallback to a label scan
            }
        }
        return getNodesByLabelAndPropertyWithoutPropertyIndex(labelId, queries);
    }

    /**
     * Find an ONLINE index that matches the schema.
     */
    private IndexDescriptor findUsableMatchingCompositeIndex(
            SchemaDescriptor schemaDescriptor,
            int[] propertyIds,
            Supplier<Iterator<IndexDescriptor>> indexesSupplier,
            IndexQuery... query) {
        // Try a direct schema match first.
        var directMatch = findUsableMatchingIndex(schemaDescriptor, query);
        if (directMatch != IndexDescriptor.NO_INDEX) {
            return directMatch;
        }

        // Attempt to find matching index with different property order
        Arrays.sort(propertyIds);
        assertNoDuplicates(propertyIds, tokenRead());

        int[] workingCopy = new int[propertyIds.length];

        Iterator<IndexDescriptor> indexes = indexesSupplier.get();
        while (indexes.hasNext()) {
            IndexDescriptor index = indexes.next();
            int[] original = index.schema().getPropertyIds();
            if (hasSamePropertyIds(original, workingCopy, propertyIds)
                    && indexIsOnline(schemaRead(), index)
                    && indexSupportQuery(index, query)) {
                // Ha! We found an index with the same properties in another order
                return index;
            }
        }

        // No dice.
        return IndexDescriptor.NO_INDEX;
    }

    private ResourceIterator<Relationship> relationshipsByTypeAndPropertyWithFiltering(
            int typeId, PropertyIndexQuery query, IndexDescriptor index, PropertyIndexQuery originalQuery) {
        Read read = dataRead();
        try {

            var cursor = cursors().allocateRelationshipValueIndexCursor(cursorContext(), memoryTracker());
            IndexReadSession indexSession = read.indexReadSession(index);
            read.relationshipIndexSeek(queryContext(), indexSession, cursor, unorderedValues(), query);

            return new TrackedCursorIterator<>(
                    new FilteringCursor<>(cursor, originalQuery),
                    value -> cursor.relationshipReference(),
                    c -> newRelationshipEntity(cursor.relationshipReference()),
                    resourceMonitor());
        } catch (KernelException e) {
            // weird at this point but ignore and fallback to a type scan
        }

        return getRelationshipsByTypeAndPropertyWithoutPropertyIndex(typeId, query);
    }

    private ResourceIterator<Relationship> getRelationshipsByTypeAndPropertyWithoutPropertyIndex(
            int typeId, PropertyIndexQuery... queries) {
        TokenPredicate tokenQuery = new TokenPredicate(typeId);
        var index = findUsableMatchingIndex(ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR, tokenQuery);

        if (index != IndexDescriptor.NO_INDEX) {
            try {
                var session = dataRead().tokenReadSession(index);
                var cursor = cursors().allocateRelationshipTypeIndexCursor(cursorContext(), memoryTracker());
                dataRead().relationshipTypeScan(session, cursor, unconstrained(), tokenQuery, cursorContext());

                var relationshipScanCursor = cursors().allocateRelationshipScanCursor(cursorContext(), memoryTracker());
                var propertyCursor = cursors().allocatePropertyCursor(cursorContext(), memoryTracker());

                return new RelationshipTypePropertyIterator(
                        dataRead(),
                        cursor,
                        relationshipScanCursor,
                        propertyCursor,
                        c -> newRelationshipEntity(c.relationshipReference()),
                        resourceMonitor(),
                        queries);
            } catch (KernelException e) {
                // ignore, fallback to all node scan
            }
        }

        return getRelationshipsByTypeAndPropertyViaAllRelsScan(typeId, queries);
    }

    private ResourceIterator<Relationship> getRelationshipsByTypeAndPropertyViaAllRelsScan(
            int typeId, PropertyIndexQuery[] queries) {
        var relationshipScanCursor = cursors().allocateRelationshipScanCursor(cursorContext(), memoryTracker());
        var typeFiltered =
                new FilteringRelationshipScanCursorWrapper(relationshipScanCursor, CursorPredicates.hasType(typeId));

        var propertyCursor = cursors().allocatePropertyCursor(cursorContext(), memoryTracker());
        var propertyFilteredCursor = new FilteringRelationshipScanCursorWrapper(
                typeFiltered, relationshipMatchProperties(queries, propertyCursor), List.of(propertyCursor));

        dataRead().allRelationshipsScan(relationshipScanCursor);
        return new TrackedCursorIterator<>(
                propertyFilteredCursor,
                RelationshipScanCursor::relationshipReference,
                c -> newRelationshipEntity(
                        c.relationshipReference(), c.sourceNodeReference(), c.type(), c.targetNodeReference()),
                resourceMonitor());
    }

    private ResourceIterator<Relationship> relationshipsByTypeAndProperty(
            int typeId, PropertyIndexQuery query, IndexDescriptor index) {
        Read read = dataRead();

        if (index != IndexDescriptor.NO_INDEX) {
            // Ha! We found an index - let's use it to find matching relationships
            try {
                var cursor = cursors().allocateRelationshipValueIndexCursor(cursorContext(), memoryTracker());
                IndexReadSession indexSession = read.indexReadSession(index);
                read.relationshipIndexSeek(queryContext(), indexSession, cursor, unconstrained(), query);

                return new TrackedCursorIterator<>(
                        cursor,
                        RelationshipIndexCursor::relationshipReference,
                        c -> newRelationshipEntity(c.relationshipReference()),
                        resourceMonitor());
            } catch (KernelException e) {
                // weird at this point but ignore and fallback to a type scan
            }
        }

        return getRelationshipsByTypeAndPropertyWithoutPropertyIndex(typeId, query);
    }

    private ResourceIterator<Relationship> relationshipsByTypeAndProperties(
            int typeId, PropertyIndexQuery.ExactPredicate... queries) {
        Read read = dataRead();

        if (isInvalidQuery(typeId, queries)) {
            return emptyResourceIterator();
        }

        int[] propertyIds = getPropertyIds(queries);
        IndexDescriptor index = findUsableMatchingCompositeIndex(
                SchemaDescriptors.forRelType(typeId, propertyIds),
                propertyIds,
                () -> schemaRead().indexesGetForRelationshipType(typeId),
                queries);

        if (index != IndexDescriptor.NO_INDEX) {
            try {
                RelationshipValueIndexCursor cursor =
                        cursors().allocateRelationshipValueIndexCursor(cursorContext(), memoryTracker());
                IndexReadSession indexSession = read.indexReadSession(index);
                read.relationshipIndexSeek(
                        queryContext(),
                        indexSession,
                        cursor,
                        unconstrained(),
                        getReorderedIndexQueries(index.schema().getPropertyIds(), queries));
                return new TrackedCursorIterator<>(
                        cursor,
                        RelationshipIndexCursor::relationshipReference,
                        c -> newRelationshipEntity(c.relationshipReference()),
                        resourceMonitor());
            } catch (KernelException e) {
                // weird at this point but ignore and fallback to a label scan
            }
        }
        return getRelationshipsByTypeAndPropertyWithoutPropertyIndex(typeId, queries);
    }

    private <T> Iterable<T> allInUse(TokenAccess<T> tokens) {
        return () -> tokens.inUse(dataRead(), schemaRead(), tokenRead());
    }

    private <T> Iterable<T> all(TokenAccess<T> tokens) {
        return () -> tokens.all(tokenRead());
    }

    /**
     * @return True if the index is online. False if the index was not found or in other state.
     */
    private static boolean indexIsOnline(SchemaRead schemaRead, IndexDescriptor index) {
        InternalIndexState state = InternalIndexState.FAILED;
        try {
            state = schemaRead.indexGetState(index);
        } catch (IndexNotFoundKernelException e) {
            // Well the index should always exist here, but if we didn't find it while checking the state,
            // then we obviously don't want to use it.
        }
        return state == InternalIndexState.ONLINE;
    }

    private static boolean indexSupportQuery(IndexDescriptor index, IndexQuery[] query) {
        return stream(query).allMatch(q -> index.getCapability().isQuerySupported(q.type(), q.valueCategory()));
    }

    private static void checkPropertyKey(String key) {
        checkArgument(key != null, "Property key can not be null");
    }

    private static void checkLabel(Label label) {
        checkArgument(label != null, "Label can not be null");
    }

    private static boolean invalidTokens(int... tokens) {
        return stream(tokens).anyMatch(token -> token == TokenRead.NO_TOKEN);
    }

    private static PropertyIndexQuery getIndexQuery(String value, StringSearchMode searchMode, int propertyId) {
        return switch (searchMode) {
            case EXACT -> PropertyIndexQuery.exact(propertyId, utf8Value(value.getBytes(UTF_8)));
            case PREFIX -> PropertyIndexQuery.stringPrefix(propertyId, utf8Value(value.getBytes(UTF_8)));
            case SUFFIX -> PropertyIndexQuery.stringSuffix(propertyId, utf8Value(value.getBytes(UTF_8)));
            case CONTAINS -> PropertyIndexQuery.stringContains(propertyId, utf8Value(value.getBytes(UTF_8)));
        };
    }

    private static boolean isInvalidQuery(int tokenId, PropertyIndexQuery[] queries) {
        if (tokenId == TokenRead.NO_TOKEN) {
            return true;
        }
        return stream(queries)
                .mapToInt(PropertyIndexQuery::propertyKeyId)
                .anyMatch(propertyKeyId -> propertyKeyId == TokenRead.NO_TOKEN);
    }

    private static PropertyIndexQuery.ExactPredicate[] convertToQueries(
            Map<String, Object> propertyValues, TokenRead tokenRead) {
        PropertyIndexQuery.ExactPredicate[] queries = new PropertyIndexQuery.ExactPredicate[propertyValues.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : propertyValues.entrySet()) {
            queries[i++] =
                    PropertyIndexQuery.exact(tokenRead.propertyKey(entry.getKey()), Values.of(entry.getValue(), false));
        }
        return queries;
    }

    private static int[] getPropertyIds(PropertyIndexQuery[] queries) {
        int[] propertyIds = new int[queries.length];
        for (int i = 0; i < queries.length; i++) {
            propertyIds[i] = queries[i].propertyKeyId();
        }
        return propertyIds;
    }

    private static void assertNoDuplicates(int[] propertyIds, TokenRead tokenRead) {
        int prev = propertyIds[0];
        for (int i = 1; i < propertyIds.length; i++) {
            int curr = propertyIds[i];
            if (curr == prev) {
                throw new IllegalArgumentException(format(
                        "Provided two queries for property %s. Only one query per property key can be performed",
                        tokenRead.propertyKeyGetName(curr)));
            }
            prev = curr;
        }
    }

    private static boolean hasSamePropertyIds(int[] original, int[] workingCopy, int[] propertyIds) {
        if (original.length == propertyIds.length) {
            System.arraycopy(original, 0, workingCopy, 0, original.length);
            Arrays.sort(workingCopy);
            return Arrays.equals(propertyIds, workingCopy);
        }
        return false;
    }

    private static PropertyIndexQuery[] getReorderedIndexQueries(int[] indexPropertyIds, PropertyIndexQuery[] queries) {
        PropertyIndexQuery[] orderedQueries = new PropertyIndexQuery[queries.length];
        for (int i = 0; i < indexPropertyIds.length; i++) {
            int propertyKeyId = indexPropertyIds[i];
            for (PropertyIndexQuery query : queries) {
                if (query.propertyKeyId() == propertyKeyId) {
                    orderedQueries[i] = query;
                    break;
                }
            }
        }
        return orderedQueries;
    }

    private static void checkRelationshipType(RelationshipType type) {
        checkArgument(type != null, "Relationship type can not be null");
    }

    protected abstract TokenRead tokenRead();

    protected abstract SchemaRead schemaRead();

    protected abstract Read dataRead();

    protected abstract ResourceMonitor resourceMonitor();

    protected abstract Node newNodeEntity(long nodeId);

    protected abstract Relationship newRelationshipEntity(long relationshipId);

    protected abstract Relationship newRelationshipEntity(long id, long startNodeId, int typeId, long endNodeId);

    protected abstract CursorFactory cursors();

    protected abstract CursorContext cursorContext();

    protected abstract MemoryTracker memoryTracker();

    protected abstract QueryContext queryContext();

    protected abstract ElementIdMapper elementIdMapper();

    protected abstract void performCheckBeforeOperation();

    private static class FilteringCursor<CURSOR extends Cursor & ValueIndexCursor> implements Cursor {

        private final CURSOR originalCursor;
        private final PropertyIndexQuery filteringQuery;

        public FilteringCursor(CURSOR originalCursor, PropertyIndexQuery filteringQuery) {
            this.originalCursor = originalCursor;
            this.filteringQuery = filteringQuery;
        }

        @Override
        public void close() {
            originalCursor.close();
        }

        @Override
        public void closeInternal() {
            originalCursor.closeInternal();
        }

        @Override
        public boolean isClosed() {
            return originalCursor.isClosed();
        }

        @Override
        public void setCloseListener(CloseListener closeListener) {
            originalCursor.setCloseListener(closeListener);
        }

        @Override
        public void setToken(int token) {
            originalCursor.setToken(token);
        }

        @Override
        public int getToken() {
            return originalCursor.getToken();
        }

        @Override
        public boolean next() {
            boolean next;
            boolean acceptsValue;

            do {
                next = originalCursor.next();
                acceptsValue = next && filteringQuery.acceptsValue(originalCursor.propertyValue(0));
            } while (next && !acceptsValue);

            return next;
        }

        @Override
        public void setTracer(KernelReadTracer tracer) {
            originalCursor.setTracer(tracer);
        }

        @Override
        public void removeTracer() {
            originalCursor.removeTracer();
        }
    }
}
