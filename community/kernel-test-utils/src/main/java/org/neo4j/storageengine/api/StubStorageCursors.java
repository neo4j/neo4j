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
package org.neo4j.storageengine.api;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.neo4j.internal.helpers.collection.MapUtil.genericMap;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.forAnyEntityTokens;
import static org.neo4j.storageengine.api.LongReference.longReference;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

/**
 * Implementation of {@link StorageReader} with focus on making testing the storage read cursors easy without resorting to mocking.
 */
public class StubStorageCursors implements StorageReader {
    private static final long NO_ID = -1;

    private final AtomicLong nextPropertyId = new AtomicLong();
    private final AtomicLong nextTokenId = new AtomicLong();
    private final TokenHolder propertyKeyTokenHolder =
            new DelegatingTokenHolder((name, internal) -> toIntExact(nextTokenId.getAndIncrement()), TYPE_PROPERTY_KEY);

    private final Map<Long, NodeData> nodeData = new HashMap<>();
    private final Map<Long, PropertyData> propertyData = new HashMap<>();
    private final Map<Long, RelationshipData> relationshipData = new HashMap<>();
    private final Map<SchemaDescriptor, IndexDescriptor> indexDescriptorMap = new HashMap<>();

    private static IndexDescriptor indexDescriptor(EntityType entityType, long id) {
        IndexPrototype indexPrototype = IndexPrototype.forSchema(forAnyEntityTokens(entityType))
                .withIndexType(LOOKUP)
                .withIndexProvider(new IndexProviderDescriptor("token-lookup", "1.0"));
        indexPrototype =
                indexPrototype.withName(SchemaNameUtil.generateName(indexPrototype, new String[] {}, new String[] {}));
        return indexPrototype.materialise(id);
    }

    public StubStorageCursors withTokenIndexes() {
        indexDescriptorMap.put(ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, indexDescriptor(EntityType.NODE, 1));
        indexDescriptorMap.put(ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR, indexDescriptor(EntityType.RELATIONSHIP, 2));
        return this;
    }

    public StubStorageCursors withoutTokenIndexes() {
        indexDescriptorMap.clear();
        return this;
    }

    public NodeData withNode(long id) {
        NodeData node = new NodeData(id);
        nodeData.put(id, node);
        return node;
    }

    public RelationshipData withRelationship(long id, long startNode, int type, long endNode) {
        RelationshipData data = new RelationshipData(id, startNode, type, endNode);
        relationshipData.put(id, data);
        return data;
    }

    private long propertyIdOf(Map<String, Value> properties) {
        if (properties.isEmpty()) {
            return NO_ID;
        }
        long propertyId = nextPropertyId.incrementAndGet();
        propertyData.put(propertyId, new PropertyData(properties));
        for (String key : properties.keySet()) {
            silentGetOrCreatePropertyKey(key);
        }
        return propertyId;
    }

    private int silentGetOrCreatePropertyKey(String key) {
        try {
            return propertyKeyTokenHolder.getOrCreateId(key);
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {}

    public TokenHolder propertyKeyTokenHolder() {
        return propertyKeyTokenHolder;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public IndexDescriptor indexGetForName(String name) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean indexExists(IndexDescriptor index) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ConstraintDescriptor constraintGetForName(String name) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Collection<IndexDescriptor> valueIndexesGetRelated(int[] labels, int propertyKeyId, EntityType entityType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Collection<IndexDescriptor> valueIndexesGetRelated(
            int[] labels, int[] propertyKeyIds, EntityType entityType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] tokens, int propertyKeyId, EntityType entityType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] tokens, int[] propertyKeyIds, EntityType entityType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
            int[] changedLabels,
            int[] unchangedLabels,
            int[] propertyKeyIds,
            boolean propertyKeyListIsComplete,
            EntityType entityType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean hasRelatedSchema(int[] labels, int propertyKey, EntityType entityType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean hasRelatedSchema(int label, EntityType entityType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId(IndexDescriptor index) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema(SchemaDescriptor descriptor) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean constraintExists(ConstraintDescriptor descriptor) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel(int labelId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType(int typeId) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public IntSet[] constraintsGetPropertyTokensForLogicalKey(int token, EntityType entityType) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<IndexDescriptor> indexGetForSchema(SchemaDescriptor descriptor) {
        if (indexDescriptorMap.containsKey(descriptor)) {
            return List.of(indexDescriptorMap.get(descriptor)).iterator();
        }
        return emptyIterator();
    }

    @Override
    public IndexDescriptor indexGetForSchemaAndType(SchemaDescriptor descriptor, IndexType type) {
        if (indexDescriptorMap.containsKey(descriptor)) {
            var index = indexDescriptorMap.get(descriptor);
            if (index.getIndexType() == type) {
                return index;
            }
        }
        return null;
    }

    @Override
    public long countsForNode(int labelId, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void visitAllCounts(CountsVisitor visitor, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long estimateCountsForNode(int labelId, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long countsForRelationship(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long estimateCountsForRelationship(
            int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public long nodesGetCount(CursorContext cursorContext) {
        return nodeData.size();
    }

    @Override
    public long relationshipsGetCount(CursorContext cursorContext) {
        return relationshipData.size();
    }

    @Override
    public int labelCount() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public int propertyKeyCount() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public int relationshipTypeCount() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean nodeExists(long id, StoreCursors storeCursors) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean relationshipExists(long id, StoreCursors storeCursors) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <T> T getOrCreateSchemaDependantState(Class<T> type, Function<StorageReader, T> factory) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AllNodeScan allNodeScan() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public AllRelationshipsScan allRelationshipScan() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public StorageNodeCursor allocateNodeCursor(CursorContext cursorContext, StoreCursors storeCursors) {
        return new StubStorageNodeCursor();
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor(
            CursorContext cursorContext, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        return new StubStoragePropertyCursor();
    }

    @Override
    public StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, StoreCursors storeCursors) {
        return new StubStorageRelationshipTraversalCursor();
    }

    @Override
    public StorageRelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, StoreCursors storeCursors) {
        return new StubStorageRelationshipScanCursor();
    }

    @Override
    public StorageSchemaReader schemaSnapshot() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public TokenNameLookup tokenNameLookup() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public static class Data<SELF> {
        boolean inUse = true;

        public SELF inUse(boolean inUse) {
            this.inUse = inUse;
            return (SELF) this;
        }
    }

    class EntityData<SELF> extends Data<SELF> {
        long propertyId = Read.NO_ID;

        public SELF propertyId(long propertyId) {
            this.propertyId = propertyId;
            return (SELF) this;
        }

        public SELF properties(Map<String, Value> properties) {
            return propertyId(propertyIdOf(properties));
        }

        public SELF properties(Object... properties) {
            return properties(genericMap(properties));
        }
    }

    public class NodeData extends EntityData<NodeData> {
        private final long id;
        private int[] labels = EMPTY_INT_ARRAY;
        private long firstRelationship = NO_ID;

        NodeData(long id) {
            this.id = id;
        }

        public NodeData labels(int... labels) {
            this.labels = labels;
            return this;
        }

        public NodeData relationship(long firstRelationship) {
            this.firstRelationship = firstRelationship;
            return this;
        }

        public long getId() {
            return id;
        }
    }

    public class RelationshipData extends EntityData<RelationshipData> {
        private final long id;
        private final long startNode;
        private final int type;
        private final long endNode;

        RelationshipData(long id, long startNode, int type, long endNode) {
            this.id = id;
            this.startNode = startNode;
            this.type = type;
            this.endNode = endNode;
        }

        RelationshipDirection direction(long nodeReference) {
            if (nodeReference == startNode) {
                return nodeReference == endNode ? RelationshipDirection.LOOP : RelationshipDirection.OUTGOING;
            } else if (nodeReference == endNode) {
                return RelationshipDirection.INCOMING;
            }
            throw new IllegalArgumentException(format(
                    "Relationship (%d)--[%d]->(%d): is not connected to node:%d",
                    startNode, type, endNode, nodeReference));
        }
    }

    private static class PropertyData {
        private final Map<String, Value> properties;

        PropertyData(Map<String, Value> properties) {
            this.properties = properties;
        }
    }

    private class StubStorageNodeCursor implements StorageNodeCursor {
        private long next;
        private NodeData current;
        private Iterator<Long> iterator;

        @Override
        public void scan() {
            this.iterator = nodeData.keySet().iterator();
            this.current = null;
        }

        @Override
        public void single(long reference) {
            this.iterator = null;
            this.next = reference;
        }

        @Override
        public boolean scanBatch(AllNodeScan scan, long sizeHint) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long entityReference() {
            return current.id;
        }

        @Override
        public int[] labels() {
            return current.labels;
        }

        @Override
        public boolean hasLabel(int label) {
            return contains(current.labels, label);
        }

        @Override
        public boolean hasLabel() {
            return current.labels.length > 0;
        }

        @Override
        public boolean hasProperties() {
            return current.propertyId != NO_ID;
        }

        @Override
        public boolean supportsFastRelationshipsTo() {
            return false;
        }

        @Override
        public void relationshipsTo(
                StorageRelationshipTraversalCursor traversalCursor,
                RelationshipSelection selection,
                long neighbourNodeReference) {
            traversalCursor.init(current.id, NO_ID, selection);
        }

        @Override
        public void relationships(StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection) {
            traversalCursor.init(current.id, NO_ID, selection);
        }

        @Override
        public int[] relationshipTypes() {
            return relationshipData.values().stream()
                    .filter(rel -> rel.startNode == current.id || rel.endNode == current.id)
                    .mapToInt(rel -> rel.type)
                    .distinct()
                    .sorted()
                    .toArray();
        }

        @Override
        public void degrees(RelationshipSelection selection, Degrees.Mutator mutator) {
            MutableIntObjectMap<int[]> degreesMap = IntObjectMaps.mutable.empty();
            relationshipData.values().stream()
                    .filter(rel -> rel.startNode == current.id || rel.endNode == current.id)
                    .filter(rel -> selection.test(rel.type, rel.direction(current.id)))
                    .forEach(rel -> degreesMap
                            .getIfAbsentPut(rel.type, () -> new int[3])[
                            rel.direction(current.id).ordinal()]++);
            degreesMap.forEachKeyValue((type, degrees) -> mutator.add(
                    type,
                    degrees[RelationshipDirection.OUTGOING.ordinal()],
                    degrees[RelationshipDirection.INCOMING.ordinal()],
                    degrees[RelationshipDirection.LOOP.ordinal()]));
        }

        @Override
        public long relationshipsReference() {
            return current.firstRelationship;
        }

        @Override
        public Reference propertiesReference() {
            return longReference(current.propertyId);
        }

        @Override
        public void properties(StoragePropertyCursor propertyCursor, PropertySelection selection) {
            propertyCursor.initNodeProperties(propertiesReference(), selection);
        }

        @Override
        public boolean next() {
            if (iterator != null) {
                // scan
                while (iterator.hasNext()) {
                    current = nodeData.get(iterator.next());
                    if (current.inUse) {
                        return true;
                    }
                }
                current = null;
                return false;
            } else {
                if (next != NO_ID) {
                    current = nodeData.get(next);
                    next = NO_ID;
                    return current != null && current.inUse;
                }
            }
            return false;
        }

        @Override
        public void reset() {
            iterator = null;
            current = null;
        }

        @Override
        public boolean supportsFastDegreeLookup() {
            return false;
        }

        @Override
        public void setForceLoad() {}

        @Override
        public void close() {
            reset();
        }
    }

    private class StubStorageRelationshipScanCursor implements StorageRelationshipScanCursor {
        private Iterator<Long> iterator;
        private RelationshipData current;
        private long next;

        @Override
        public void scan() {
            iterator = relationshipData.keySet().iterator();
            next = NO_ID;
        }

        @Override
        public void single(long reference) {
            iterator = null;
            next = reference;
        }

        @Override
        public void single(long reference, long sourceNodeReference, int type, long targetNodeReference) {
            single(reference);
        }

        @Override
        public boolean scanBatch(AllRelationshipsScan scan, long sizeHint) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long entityReference() {
            return current.id;
        }

        @Override
        public int type() {
            return current.type;
        }

        @Override
        public boolean hasProperties() {
            return current.propertyId != NO_ID;
        }

        @Override
        public long sourceNodeReference() {
            return current.startNode;
        }

        @Override
        public long targetNodeReference() {
            return current.endNode;
        }

        @Override
        public Reference propertiesReference() {
            return longReference(current.propertyId);
        }

        @Override
        public void properties(StoragePropertyCursor propertyCursor, PropertySelection selection) {
            propertyCursor.initRelationshipProperties(propertiesReference(), selection);
        }

        @Override
        public boolean next() {
            if (iterator != null) {
                if (!iterator.hasNext()) {
                    return false;
                }
                next = iterator.next();
            }

            if (next != NO_ID) {
                current = relationshipData.get(next);
                next = NO_ID;
                return true;
            }
            return false;
        }

        @Override
        public void reset() {
            current = null;
            next = NO_ID;
        }

        @Override
        public void setForceLoad() {}

        @Override
        public void close() {
            reset();
        }
    }

    private class StubStoragePropertyCursor implements StoragePropertyCursor {
        private Map.Entry<String, Value> current;
        private Iterator<Map.Entry<String, Value>> iterator;

        @Override
        public void initNodeProperties(Reference reference, PropertySelection selection, long ownerReference) {
            init(reference, selection);
        }

        @Override
        public void initRelationshipProperties(Reference reference, PropertySelection selection, long ownerReference) {
            init(reference, selection);
        }

        private void init(Reference reference, PropertySelection selection) {
            long id = ((LongReference) reference).id;
            PropertyData properties = StubStorageCursors.this.propertyData.get(id);
            iterator = properties != null ? properties.properties.entrySet().iterator() : emptyIterator();
            iterator = Iterators.filter(p -> selection.test(propertyKeyTokenHolder.getIdByName(p.getKey())), iterator);
        }

        @Override
        public void close() {}

        @Override
        public int propertyKey() {
            return silentGetOrCreatePropertyKey(current.getKey());
        }

        @Override
        public ValueGroup propertyType() {
            return current.getValue().valueGroup();
        }

        @Override
        public Value propertyValue() {
            return current.getValue();
        }

        @Override
        public void reset() {}

        @Override
        public void setForceLoad() {}

        @Override
        public boolean next() {
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            }
            return false;
        }
    }

    private class StubStorageRelationshipTraversalCursor implements StorageRelationshipTraversalCursor {
        private Iterator<RelationshipData> iterator;
        private RelationshipData current;
        private long originNodeReference;

        @Override
        public boolean next() {
            if (!iterator.hasNext()) {
                return false;
            }
            current = iterator.next();
            return true;
        }

        @Override
        public void reset() {
            iterator = null;
            current = null;
        }

        @Override
        public void setForceLoad() {}

        @Override
        public void close() {}

        @Override
        public boolean hasProperties() {
            return current.propertyId != NO_ID;
        }

        @Override
        public Reference propertiesReference() {
            return longReference(current.propertyId);
        }

        @Override
        public void properties(StoragePropertyCursor propertyCursor, PropertySelection selection) {
            propertyCursor.initRelationshipProperties(propertiesReference(), selection);
        }

        @Override
        public long entityReference() {
            return current.id;
        }

        @Override
        public int type() {
            return current.type;
        }

        @Override
        public long sourceNodeReference() {
            return current.startNode;
        }

        @Override
        public long targetNodeReference() {
            return current.endNode;
        }

        @Override
        public long neighbourNodeReference() {
            return originNodeReference == current.startNode ? current.endNode : current.startNode;
        }

        @Override
        public long originNodeReference() {
            return originNodeReference;
        }

        @Override
        public void init(long nodeReference, long reference, RelationshipSelection selection) {
            originNodeReference = nodeReference;
            iterator = relationshipData.values().stream()
                    .filter(relationship ->
                            relationship.startNode == nodeReference || relationship.endNode == nodeReference)
                    .filter(relationship -> selection.test(relationship.type, relationship.direction(nodeReference)))
                    .iterator();
        }
    }
}
