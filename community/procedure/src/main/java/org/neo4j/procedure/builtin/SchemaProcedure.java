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
package org.neo4j.procedure.builtin;

import static org.neo4j.internal.helpers.collection.Iterators.stream;
import static org.neo4j.kernel.impl.api.TokenAccess.LABELS;
import static org.neo4j.kernel.impl.api.TokenAccess.RELATIONSHIP_TYPES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.neo4j.collection.trackable.HeapTracking;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.AbstractVirtualNode;
import org.neo4j.kernel.impl.core.AbstractVirtualRelationship;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.PropertyNameUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.memory.ProcedureMemory;
import org.neo4j.procedure.memory.ProcedureMemory.HeapTrackingCollectionFactory;
import org.neo4j.procedure.memory.ProcedureMemoryTracker;
import org.neo4j.token.api.TokenConstants;

public class SchemaProcedure {
    private final InternalTransaction internalTransaction;
    private final ProcedureMemory memory;
    private final ProcedureMemoryTracker tracker;

    // Dummy value to associate with an Object in a map that represents a set
    static final Object PRESENT = new Object();

    static final long MEMORY_ESTIMATION_COUNT_STORE_LOOKUP = 25;
    static final long MEMORY_ESTIMATION_FACTOR_VIRTUAL_NODE = 50;
    static final long MEMORY_ESTIMATION_FACTOR_VIRTUAL_RELATIONSHIP = 50;

    public SchemaProcedure(
            InternalTransaction internalTransaction, ProcedureMemory memory, ProcedureMemoryTracker tracker) {
        this.internalTransaction = internalTransaction;
        this.memory = memory;
        this.tracker = tracker;
        tracker.allocateHeap(memory.heapEstimator().shallowSizeOfInstance(SchemaProcedure.class));
    }

    public GraphResult buildSchemaGraph() {
        HeapTrackingCollectionFactory collections = memory.collections();
        HeapTracking.Map<String, VirtualNodeHack> nodes = collections.newHeapTrackingUnifiedMap();
        HeapTracking.Map<String, Map<VirtualRelationshipHack, Object>> relationships =
                collections.newHeapTrackingUnifiedMap();
        KernelTransaction kernelTransaction = internalTransaction.kernelTransaction();
        AccessMode mode = kernelTransaction.securityContext().mode();

        try (KernelTransaction.Revertable ignore = kernelTransaction.overrideWith(SecurityContext.AUTH_DISABLED)) {
            Read dataRead = kernelTransaction.dataRead();
            TokenRead tokenRead = kernelTransaction.tokenRead();
            SchemaRead schemaRead = kernelTransaction.schemaRead();

            HeapTracking.List<LabelNameId> labelNamesAndIds = collections.newHeapTrackingArrayList();

            // Get all labels that are in use as seen by a super user
            HeapTracking.List<Label> labelsInUse = collections.newHeapTrackingArrayList();
            labelsInUse.addAll(stream(LABELS.inUse(
                            kernelTransaction.dataRead(),
                            kernelTransaction.schemaRead(),
                            kernelTransaction.tokenRead()))
                    .toList());

            for (Label label : labelsInUse) {
                String labelName = label.name();
                int labelId = tokenRead.nodeLabel(labelName);

                tracker.allocateHeap(MEMORY_ESTIMATION_COUNT_STORE_LOOKUP);
                // Filter out labels that are denied or aren't explicitly allowed
                if (mode.allowsTraverseNode(labelId) && dataRead.estimateCountsForNode(labelId) > 0) {
                    labelNamesAndIds.add(new LabelNameId(labelName, labelId));

                    HeapTracking.Map<String, Object> properties = collections.newHeapTrackingUnifiedMap();

                    Iterator<IndexDescriptor> indexReferences = schemaRead.indexesGetForLabel(labelId);
                    List<String> indexes = new ArrayList<>();
                    while (indexReferences.hasNext()) {
                        IndexDescriptor index = indexReferences.next();
                        if (!index.isUnique()) {
                            tracker.allocateHeap(memory.heapEstimator()
                                    .shallowSizeOfObjectArray(index.schema().getPropertyIds().length));
                            String[] propertyNames = PropertyNameUtils.getPropertyKeys(
                                    tokenRead, index.schema().getPropertyIds());
                            String indexDescription = String.join(",", propertyNames);
                            tracker.allocateHeap(
                                    memory.heapEstimator().sizeOfByteArray(indexDescription.getBytes().length));
                            indexes.add(indexDescription);
                        }
                    }
                    properties.put("indexes", indexes);

                    Iterator<ConstraintDescriptor> nodePropertyConstraintIterator =
                            schemaRead.constraintsGetForLabel(labelId);
                    List<String> constraints = new ArrayList<>();
                    while (nodePropertyConstraintIterator.hasNext()) {
                        ConstraintDescriptor constraint = nodePropertyConstraintIterator.next();
                        String constraintDescription = constraint.userDescription(tokenRead);
                        tracker.allocateHeap(
                                memory.heapEstimator().sizeOfByteArray(constraintDescription.getBytes().length));
                        constraints.add(constraintDescription);
                    }
                    properties.put("constraints", constraints);

                    getOrCreateLabel(label.name(), properties, nodes);
                }
            }

            // Get all relTypes that are in use as seen by a super user
            List<RelationshipType> relTypesInUse = stream(RELATIONSHIP_TYPES.inUse(
                            kernelTransaction.dataRead(),
                            kernelTransaction.schemaRead(),
                            kernelTransaction.tokenRead()))
                    .toList();

            Map<String, Object> emptyProperties = new HashMap<>();
            for (RelationshipType relationshipType : relTypesInUse) {
                String relationshipTypeGetName = relationshipType.name();
                int relId = tokenRead.relationshipType(relationshipTypeGetName);

                // Filter out relTypes that are denied or aren't explicitly allowed
                if (mode.allowsTraverseRelType(relId)) {
                    HeapTracking.List<VirtualNodeHack> startNodes = collections.newHeapTrackingArrayList();
                    HeapTracking.List<VirtualNodeHack> endNodes = collections.newHeapTrackingArrayList();

                    for (LabelNameId labelNameAndId : labelNamesAndIds) {
                        tracker.allocateHeap(MEMORY_ESTIMATION_COUNT_STORE_LOOKUP);

                        String labelName = labelNameAndId.name();
                        int labelId = labelNameAndId.id();

                        VirtualNodeHack node = getOrCreateLabel(labelName, emptyProperties, nodes);

                        if (dataRead.estimateCountsForRelationships(labelId, relId, TokenConstants.ANY_LABEL) > 0) {
                            startNodes.add(node);
                        }
                        if (dataRead.estimateCountsForRelationships(TokenConstants.ANY_LABEL, relId, labelId) > 0) {
                            endNodes.add(node);
                        }
                    }

                    for (VirtualNodeHack startNode : startNodes) {
                        for (VirtualNodeHack endNode : endNodes) {
                            addRelationship(startNode, endNode, relationshipTypeGetName, relationships);
                        }
                    }

                    startNodes.close();
                    endNodes.close();
                }
            }

            labelNamesAndIds.close();
        }
        return getGraphResult(nodes, relationships);
    }

    private record LabelNameId(String name, int id) {}

    public record GraphResult(
            @Description("A list of virtual nodes representing each label in the database.")
            List<Node> nodes,

            @Description(
                    "A list of virtual relationships representing all combinations between start and end nodes in the"
                            + " database.")
            List<Relationship> relationships) {}

    private VirtualNodeHack getOrCreateLabel(
            String label, Map<String, Object> properties, Map<String, VirtualNodeHack> nodeMap) {
        if (nodeMap.containsKey(label)) {
            return nodeMap.get(label);
        }
        tracker.allocateHeap(MEMORY_ESTIMATION_FACTOR_VIRTUAL_NODE
                * memory.heapEstimator().shallowSizeOfInstance(VirtualNodeHack.class));
        VirtualNodeHack node = new VirtualNodeHack(label, properties);
        nodeMap.put(label, node);
        return node;
    }

    private void addRelationship(
            VirtualNodeHack startNode,
            VirtualNodeHack endNode,
            String relType,
            Map<String, Map<VirtualRelationshipHack, Object>> relationshipMap) {
        Map<VirtualRelationshipHack, Object> relationshipsForType;
        if (!relationshipMap.containsKey(relType)) {
            relationshipsForType = memory.collections().newHeapTrackingUnifiedMap();
            relationshipMap.put(relType, relationshipsForType);
        } else {
            relationshipsForType = relationshipMap.get(relType);
        }
        tracker.allocateHeap(MEMORY_ESTIMATION_FACTOR_VIRTUAL_RELATIONSHIP
                * memory.heapEstimator().shallowSizeOfInstance(VirtualRelationshipHack.class));
        VirtualRelationshipHack relationship = new VirtualRelationshipHack(startNode, endNode, relType);
        relationshipsForType.put(relationship, PRESENT);
    }

    private GraphResult getGraphResult(
            Map<String, VirtualNodeHack> nodeMap, Map<String, Map<VirtualRelationshipHack, Object>> relationshipMap) {
        List<Relationship> relationships = new ArrayList<>();
        for (Map<VirtualRelationshipHack, Object> relationship : relationshipMap.values()) {
            relationships.addAll(relationship.keySet());
        }

        tracker.allocateHeap(memory.heapEstimator().shallowSizeOfInstance(GraphResult.class));
        tracker.allocateHeap(memory.heapEstimator().shallowSizeOfInstance(ArrayList.class));
        tracker.allocateHeap(memory.heapEstimator().shallowSizeOfObjectArray(nodeMap.size()));

        return new GraphResult(new ArrayList<>(nodeMap.values()), relationships);
    }

    public static Relationship virtualRelationshipOf(String type, Node from, Node to) {
        return new VirtualRelationshipHack(from, to, type);
    }

    public static Node virtualNodeOf(String label, Map<String, Object> properties) {
        return new VirtualNodeHack(label, properties);
    }

    private static class VirtualRelationshipHack extends AbstractVirtualRelationship {

        private final long id;
        private final Node startNode;
        private final Node endNode;
        private final RelationshipType relationshipType;
        private final Map<String, Object> propertyMap = HashMap.newHashMap(1);

        private VirtualRelationshipHack(Node startNode, Node endNode, String type) {
            this.id = MIN_ID.getAndDecrement();
            this.startNode = startNode;
            this.endNode = endNode;
            propertyMap.put("name", type);
            relationshipType = () -> type;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public String getElementId() {
            return String.valueOf(id);
        }

        @Override
        public Node getStartNode() {
            return startNode;
        }

        @Override
        public Node getEndNode() {
            return endNode;
        }

        @Override
        public RelationshipType getType() {
            return relationshipType;
        }

        @Override
        public Map<String, Object> getAllProperties() {
            return propertyMap;
        }

        @Override
        public Node getOtherNode(Node node) {
            return null;
        }

        @Override
        public Node[] getNodes() {
            return new Node[0];
        }

        @Override
        public boolean isType(RelationshipType type) {
            return false;
        }

        @Override
        public boolean hasProperty(String key) {
            return false;
        }

        @Override
        public Object getProperty(String key) {
            return null;
        }

        @Override
        public Object getProperty(String key, Object defaultValue) {
            return null;
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return null;
        }

        @Override
        public Map<String, Object> getProperties(String... keys) {
            return null;
        }

        @Override
        public String toString() {
            return String.format("VirtualRelationshipHack[%s]", id);
        }
    }

    private static class VirtualNodeHack extends AbstractVirtualNode {

        private final Map<String, Object> propertyMap = new HashMap<>();

        private final long id;
        private final Label label;

        private VirtualNodeHack(String label, Map<String, Object> properties) {
            this.id = MIN_ID.getAndDecrement();
            this.label = Label.label(label);
            propertyMap.putAll(properties);
            propertyMap.put("name", label);
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public String getElementId() {
            return String.valueOf(id);
        }

        @Override
        public Map<String, Object> getAllProperties() {
            return propertyMap;
        }

        @Override
        public Iterable<Label> getLabels() {
            return Collections.singletonList(label);
        }

        @Override
        public ResourceIterable<Relationship> getRelationships() {
            return null;
        }

        @Override
        public boolean hasRelationship() {
            return false;
        }

        @Override
        public ResourceIterable<Relationship> getRelationships(RelationshipType... types) {
            return null;
        }

        @Override
        public ResourceIterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
            return null;
        }

        @Override
        public ResourceIterable<Relationship> getRelationships(Direction direction) {
            return null;
        }

        @Override
        public boolean hasRelationship(RelationshipType... types) {
            return false;
        }

        @Override
        public boolean hasRelationship(Direction direction, RelationshipType... types) {
            return false;
        }

        @Override
        public boolean hasRelationship(Direction direction) {
            return false;
        }

        @Override
        public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
            return null;
        }

        @Override
        public Iterable<RelationshipType> getRelationshipTypes() {
            return null;
        }

        @Override
        public int getDegree() {
            return 0;
        }

        @Override
        public int getDegree(RelationshipType type) {
            return 0;
        }

        @Override
        public int getDegree(RelationshipType type, Direction direction) {
            return 0;
        }

        @Override
        public int getDegree(Direction direction) {
            return 0;
        }

        @Override
        public boolean hasLabel(Label label) {
            return false;
        }

        @Override
        public boolean hasProperty(String key) {
            return false;
        }

        @Override
        public Object getProperty(String key) {
            return null;
        }

        @Override
        public Object getProperty(String key, Object defaultValue) {
            return null;
        }

        @Override
        public Iterable<String> getPropertyKeys() {
            return null;
        }

        @Override
        public Map<String, Object> getProperties(String... keys) {
            return null;
        }

        @Override
        public String toString() {
            return String.format("VirtualNodeHack[%s]", id);
        }
    }
}
