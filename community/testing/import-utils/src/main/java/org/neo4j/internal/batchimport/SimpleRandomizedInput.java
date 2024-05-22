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
package org.neo4j.internal.batchimport;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.batchimport.input.DataGeneratorInput.bareboneIncrementalNodeHeader;
import static org.neo4j.internal.batchimport.input.DataGeneratorInput.bareboneNodeHeader;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.neo4j.common.EntityType;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.DataGeneratorInput;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.input.csv.Header;
import org.neo4j.internal.batchimport.input.csv.Header.Entry;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SimpleRandomizedInput implements Input {
    public static final String ID_KEY = "id";

    private final Input actual;
    private final long nodeCount;
    private final long relationshipCount;

    public SimpleRandomizedInput(long seed, long nodeCount, long relationshipCount) {
        this(seed, DataGeneratorInput.data(nodeCount, relationshipCount), 0, 0, null);
    }

    public SimpleRandomizedInput(
            long seed,
            DataGeneratorInput.DataDistribution dataDistribution,
            int maxAdditionalNodeProperties,
            int maxAdditionalRelationshipProperties,
            String labelNameForIncrementalImport) {
        this.nodeCount = dataDistribution.nodeCount();
        this.relationshipCount = dataDistribution.relationshipCount();
        var idType = IdType.INTEGER;
        var extractors = new Extractors(Configuration.COMMAS.arrayDelimiter());
        var groups = new Groups();
        var group = groups.getOrCreate(null);

        List<Entry> additionalRelationshipEntries = new ArrayList<>();
        additionalRelationshipEntries.add(
                new Entry(SimpleRandomizedInput.ID_KEY, Type.PROPERTY, null, extractors.int_()));
        additionalRelationshipEntries.addAll(
                Arrays.asList(additionalPropertyEntries(maxAdditionalRelationshipProperties, group, extractors, seed)));

        Entry[] additionalNodeHeaderEntries =
                additionalPropertyEntries(maxAdditionalNodeProperties, group, extractors, seed);
        Header nodeHeader = labelNameForIncrementalImport != null
                ? bareboneIncrementalNodeHeader(
                        ID_KEY, labelNameForIncrementalImport, idType, group, extractors, additionalNodeHeaderEntries)
                : bareboneNodeHeader(ID_KEY, idType, group, extractors, additionalNodeHeaderEntries);
        actual = new DataGeneratorInput(
                dataDistribution,
                idType,
                seed,
                nodeHeader,
                DataGeneratorInput.bareboneRelationshipHeader(
                        idType, group, extractors, additionalRelationshipEntries.toArray(new Entry[0])),
                groups);
    }

    private Entry[] additionalPropertyEntries(int count, Group group, Extractors extractors, long seed) {
        var entries = new Entry[count];
        var rng = new Random(seed);
        for (int i = 0; i < count; i++) {
            entries[i] = new Entry("p" + i, Type.PROPERTY, group, randomType(extractors, rng));
        }
        return entries;
    }

    private Extractor<?> randomType(Extractors extractors, Random random) {
        return switch (random.nextInt(12)) {
            case 0 -> extractors.byte_();
            case 1 -> extractors.short_();
            case 2 -> extractors.int_();
            case 3 -> extractors.long_();
            case 4 -> extractors.string();
            case 5 -> extractors.boolean_();
            case 6 -> extractors.byteArray();
            case 7 -> extractors.shortArray();
            case 8 -> extractors.intArray();
            case 9 -> extractors.longArray();
            case 10 -> extractors.stringArray();
            case 11 -> extractors.booleanArray();
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return actual.nodes(badCollector);
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return actual.relationships(badCollector);
    }

    @Override
    public IdType idType() {
        return actual.idType();
    }

    @Override
    public ReadableGroups groups() {
        return actual.groups();
    }

    @Override
    public Map<String, SchemaDescriptor> referencedNodeSchema(TokenHolders tokenHolders) {
        return actual.referencedNodeSchema(tokenHolders);
    }

    public void verify(GraphDatabaseService db) throws IOException {
        verify(db, false);
    }

    public void verifyWithTokenIndexes(GraphDatabaseService db) throws IOException {
        verify(db, true);
    }

    public void verify(GraphDatabaseService db, boolean verifyIndex) throws IOException {
        verify(db, verifyIndex, this);
    }

    public static void verify(GraphDatabaseService db, boolean verifyIndex, SimpleRandomizedInput... inputs)
            throws IOException {
        Map<Number, InputEntity> expectedNodeData = new HashMap<>();
        long numBadNodes = 0;
        long nodeCount = 0;
        for (var input : inputs) {
            nodeCount += input.nodeCount;
            try (InputIterator nodes = input.nodes(Collector.EMPTY).iterator();
                    InputChunk chunk = nodes.newChunk();
                    Transaction tx = db.beginTx()) {
                InputEntity node;
                while (nodes.next(chunk)) {
                    while (chunk.next(node = new InputEntity())) {
                        Number id = (Number) node.id();
                        if (!expectedNodeData.containsKey(id)) {
                            expectedNodeData.put(id, node);
                        } else {
                            numBadNodes++;
                        }
                    }
                }
            }
        }
        Map<RelationshipKey, Set<InputEntity>> expectedRelationshipData = new HashMap<>();
        long numBadRelationships = 0;
        long relationshipCount = 0;
        for (var input : inputs) {
            relationshipCount += input.relationshipCount;
            try (InputIterator relationships =
                            input.relationships(Collector.EMPTY).iterator();
                    InputChunk chunk = relationships.newChunk()) {
                while (relationships.next(chunk)) {
                    InputEntity relationship;
                    while (chunk.next(relationship = new InputEntity())) {
                        RelationshipKey key = new RelationshipKey(
                                relationship.startId(), relationship.stringType, relationship.endId());
                        if (key.startId != null
                                && key.type != null
                                && key.endId != null
                                && expectedNodeData.containsKey((Number) relationship.startId())
                                && expectedNodeData.containsKey((Number) relationship.endId())) {
                            expectedRelationshipData
                                    .computeIfAbsent(key, k -> new HashSet<>())
                                    .add(relationship);
                        } else {
                            numBadRelationships++;
                        }
                    }
                }
            }
        }

        try (Transaction tx = db.beginTx();
                ResourceIterable<Relationship> allRelationships = tx.getAllRelationships()) {
            long actualRelationshipCount = 0;
            for (Relationship relationship : allRelationships) {
                RelationshipKey key = keyOf(relationship);
                Set<InputEntity> matches = expectedRelationshipData.get(key);
                assertNotNull(matches);
                InputEntity matchingRelationship = relationshipWithId(matches, relationship);
                assertNotNull(matchingRelationship);
                assertTrue(matches.remove(matchingRelationship));
                if (matches.isEmpty()) {
                    expectedRelationshipData.remove(key);
                }
                actualRelationshipCount++;
            }
            if (!expectedRelationshipData.isEmpty()) {
                fail(format(
                        "Imported db is missing %d/%d relationships: %s",
                        expectedRelationshipData.size(), relationshipCount, expectedRelationshipData));
            }

            long actualNodeCount = 0;
            try (ResourceIterable<Node> allNodes = tx.getAllNodes()) {
                for (Node node : allNodes) {
                    assertNotNull(expectedNodeData.remove(node.getProperty(ID_KEY)));
                    actualNodeCount++;
                }
            }
            if (!expectedNodeData.isEmpty()) {
                fail(format(
                        "Imported db is missing %d/%d nodes: %s",
                        expectedNodeData.size(), nodeCount, expectedNodeData));
            }
            assertEquals(nodeCount - numBadNodes, actualNodeCount);
            assertEquals(relationshipCount - numBadRelationships, actualRelationshipCount);
            tx.commit();
        }

        if (verifyIndex) {
            Map<Integer, List<Long>> expectedLabelIndexData = new HashMap<>();
            Map<Integer, List<Long>> expectedRelationshipIndexData = new HashMap<>();
            try (Transaction tx = db.beginTx();
                    ResourceIterable<Node> allNodes = tx.getAllNodes();
                    ResourceIterable<Relationship> allRelationships = tx.getAllRelationships()) {
                TokenRead tokenRead =
                        ((InternalTransaction) tx).kernelTransaction().tokenRead();

                allNodes.forEach(node -> {
                    for (var label : node.getLabels()) {
                        int labelId = tokenRead.nodeLabel(label.name());
                        expectedLabelIndexData
                                .computeIfAbsent(labelId, labelToken -> new ArrayList<>())
                                .add(node.getId());
                    }
                });

                allRelationships.forEach(relationship -> {
                    int relTypeId =
                            tokenRead.relationshipType(relationship.getType().name());
                    expectedRelationshipIndexData
                            .computeIfAbsent(relTypeId, relToken -> new ArrayList<>())
                            .add(relationship.getId());
                });
            }
            verifyIndex(NODE, expectedLabelIndexData, db);
            verifyIndex(RELATIONSHIP, expectedRelationshipIndexData, db);
        }
    }

    private static void verifyIndex(EntityType entity, Map<Integer, List<Long>> expectedIds, GraphDatabaseService db) {
        try {
            try (var tx = db.beginTx()) {
                var internalTx = (InternalTransaction) tx;
                var ktx = internalTx.kernelTransaction();
                var schemaRead = ktx.schemaRead();
                var index = single(schemaRead.index(SchemaDescriptors.forAnyEntityTokens(entity)));
                var session = ktx.dataRead().tokenReadSession(index);
                try (var nodeCursor = ktx.cursors().allocateNodeLabelIndexCursor(CursorContext.NULL_CONTEXT);
                        var relationshipCursor =
                                ktx.cursors().allocateRelationshipTypeIndexCursor(CursorContext.NULL_CONTEXT)) {
                    expectedIds.forEach((tokenId, expectedEntities) -> {
                        try {
                            List<Long> actualEntities = new ArrayList<>();
                            if (entity == NODE) {
                                ktx.dataRead()
                                        .nodeLabelScan(
                                                session,
                                                nodeCursor,
                                                unconstrained(),
                                                new TokenPredicate(tokenId),
                                                CursorContext.NULL_CONTEXT);
                                while (nodeCursor.next()) {
                                    actualEntities.add(nodeCursor.nodeReference());
                                }
                            } else {
                                ktx.dataRead()
                                        .relationshipTypeScan(
                                                session,
                                                relationshipCursor,
                                                unconstrained(),
                                                new TokenPredicate(tokenId),
                                                CursorContext.NULL_CONTEXT);
                                while (relationshipCursor.next()) {
                                    actualEntities.add(relationshipCursor.relationshipReference());
                                }
                            }
                            expectedEntities.sort(Long::compareTo);
                            actualEntities.sort(Long::compareTo);
                            assertEquals(expectedEntities, actualEntities);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            }
        } catch (IndexNotFoundKernelException e) {
            throw new RuntimeException(e);
        }
    }

    private static InputEntity relationshipWithId(Set<InputEntity> matches, Relationship relationship) {
        Map<String, Value> dbProperties = toValueMap(relationship.getAllProperties());
        for (InputEntity candidate : matches) {
            if (dbProperties.equals(propertiesOf(candidate))) {
                return candidate;
            }
        }
        return null;
    }

    private static Map<String, Value> toValueMap(Map<String, Object> map) {
        Map<String, Value> result = new HashMap<>();
        map.forEach((key, value) -> result.put(key, Values.of(value)));
        return result;
    }

    private static Map<String, Value> propertiesOf(InputEntity entity) {
        Map<String, Value> result = new HashMap<>();
        Object[] properties = entity.properties();
        for (int i = 0; i < properties.length; i++) {
            result.put((String) properties[i++], Values.of(properties[i]));
        }
        return result;
    }

    private record RelationshipKey(Object startId, String type, Object endId) {}

    private static RelationshipKey keyOf(Relationship relationship) {
        return new RelationshipKey(
                relationship.getStartNode().getProperty(ID_KEY),
                relationship.getType().name(),
                relationship.getEndNode().getProperty(ID_KEY));
    }

    @Override
    public Estimates calculateEstimates(PropertySizeCalculator valueSizeCalculator) throws IOException {
        return Input.knownEstimates(nodeCount, relationshipCount, 0, 0, 0, 0, 0);
    }
}
