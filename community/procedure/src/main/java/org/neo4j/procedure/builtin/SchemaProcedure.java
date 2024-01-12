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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.PropertyNameUtils;

public class SchemaProcedure {
    private final InternalTransaction internalTransaction;

    public SchemaProcedure(final InternalTransaction internalTransaction) {
        this.internalTransaction = internalTransaction;
    }

    public GraphResult buildSchemaGraph() {
        final Map<String, VirtualNodeHack> nodes = new HashMap<>();
        final Map<String, Set<VirtualRelationshipHack>> relationships = new HashMap<>();
        final KernelTransaction kernelTransaction = internalTransaction.kernelTransaction();
        AccessMode mode = kernelTransaction.securityContext().mode();

        try (KernelTransaction.Revertable ignore = kernelTransaction.overrideWith(SecurityContext.AUTH_DISABLED)) {
            Read dataRead = kernelTransaction.dataRead();
            TokenRead tokenRead = kernelTransaction.tokenRead();
            SchemaRead schemaRead = kernelTransaction.schemaRead();

            List<LabelNameId> labelNamesAndIds = new ArrayList<>();

            // Get all labels that are in use as seen by a super user
            List<Label> labelsInUse = stream(LABELS.inUse(
                            kernelTransaction.dataRead(),
                            kernelTransaction.schemaRead(),
                            kernelTransaction.tokenRead()))
                    .toList();

            for (Label label : labelsInUse) {
                String labelName = label.name();
                int labelId = tokenRead.nodeLabel(labelName);

                // Filter out labels that are denied or aren't explicitly allowed
                if (mode.allowsTraverseNode(labelId)) {
                    labelNamesAndIds.add(new LabelNameId(labelName, labelId));

                    Map<String, Object> properties = new HashMap<>();

                    Iterator<IndexDescriptor> indexReferences = schemaRead.indexesGetForLabel(labelId);
                    List<String> indexes = new ArrayList<>();
                    while (indexReferences.hasNext()) {
                        IndexDescriptor index = indexReferences.next();
                        if (!index.isUnique()) {
                            String[] propertyNames = PropertyNameUtils.getPropertyKeys(
                                    tokenRead, index.schema().getPropertyIds());
                            indexes.add(String.join(",", propertyNames));
                        }
                    }
                    properties.put("indexes", indexes);

                    Iterator<ConstraintDescriptor> nodePropertyConstraintIterator =
                            schemaRead.constraintsGetForLabel(labelId);
                    List<String> constraints = new ArrayList<>();
                    while (nodePropertyConstraintIterator.hasNext()) {
                        ConstraintDescriptor constraint = nodePropertyConstraintIterator.next();
                        constraints.add(constraint.userDescription(tokenRead));
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

            for (RelationshipType relationshipType : relTypesInUse) {
                String relationshipTypeGetName = relationshipType.name();
                int relId = tokenRead.relationshipType(relationshipTypeGetName);

                // Filter out relTypes that are denied or aren't explicitly allowed
                if (mode.allowsTraverseRelType(relId)) {
                    List<VirtualNodeHack> startNodes = new LinkedList<>();
                    List<VirtualNodeHack> endNodes = new LinkedList<>();

                    for (LabelNameId labelNameAndId : labelNamesAndIds) {
                        String labelName = labelNameAndId.name();
                        int labelId = labelNameAndId.id();

                        Map<String, Object> properties = new HashMap<>();
                        VirtualNodeHack node = getOrCreateLabel(labelName, properties, nodes);

                        if (dataRead.estimateCountsForRelationships(labelId, relId, TokenRead.ANY_LABEL) > 0) {
                            startNodes.add(node);
                        }
                        if (dataRead.estimateCountsForRelationships(TokenRead.ANY_LABEL, relId, labelId) > 0) {
                            endNodes.add(node);
                        }
                    }

                    for (VirtualNodeHack startNode : startNodes) {
                        for (VirtualNodeHack endNode : endNodes) {
                            addRelationship(startNode, endNode, relationshipTypeGetName, relationships);
                        }
                    }
                }
            }
        }
        return getGraphResult(nodes, relationships);
    }

    private record LabelNameId(String name, int id) {}

    public record GraphResult(List<Node> nodes, List<Relationship> relationships) {}

    private static VirtualNodeHack getOrCreateLabel(
            String label, Map<String, Object> properties, final Map<String, VirtualNodeHack> nodeMap) {
        if (nodeMap.containsKey(label)) {
            return nodeMap.get(label);
        }
        VirtualNodeHack node = new VirtualNodeHack(label, properties);
        nodeMap.put(label, node);
        return node;
    }

    private static void addRelationship(
            VirtualNodeHack startNode,
            VirtualNodeHack endNode,
            String relType,
            final Map<String, Set<VirtualRelationshipHack>> relationshipMap) {
        Set<VirtualRelationshipHack> relationshipsForType;
        if (!relationshipMap.containsKey(relType)) {
            relationshipsForType = new HashSet<>();
            relationshipMap.put(relType, relationshipsForType);
        } else {
            relationshipsForType = relationshipMap.get(relType);
        }
        VirtualRelationshipHack relationship = new VirtualRelationshipHack(startNode, endNode, relType);
        relationshipsForType.add(relationship);
    }

    private static GraphResult getGraphResult(
            final Map<String, VirtualNodeHack> nodeMap,
            final Map<String, Set<VirtualRelationshipHack>> relationshipMap) {
        List<Relationship> relationships = new LinkedList<>();
        for (Set<VirtualRelationshipHack> relationship : relationshipMap.values()) {
            relationships.addAll(relationship);
        }

        GraphResult graphResult;
        graphResult = new GraphResult(new ArrayList<>(nodeMap.values()), relationships);

        return graphResult;
    }

    private static class VirtualRelationshipHack implements Relationship {

        private static final AtomicLong MIN_ID = new AtomicLong(-1);

        private final long id;
        private final Node startNode;
        private final Node endNode;
        private final RelationshipType relationshipType;
        private final Map<String, Object> propertyMap = new HashMap<>();

        VirtualRelationshipHack(final VirtualNodeHack startNode, final VirtualNodeHack endNode, final String type) {
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
        public void delete() {}

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
        public void setProperty(String key, Object value) {}

        @Override
        public Object removeProperty(String key) {
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

    private static class VirtualNodeHack implements Node {

        private final Map<String, Object> propertyMap = new HashMap<>();

        private static final AtomicLong MIN_ID = new AtomicLong(-1);
        private final long id;
        private final Label label;

        VirtualNodeHack(final String label, Map<String, Object> properties) {
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
        public void delete() {}

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
        public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
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
        public void addLabel(Label label) {}

        @Override
        public void removeLabel(Label label) {}

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
        public void setProperty(String key, Object value) {}

        @Override
        public Object removeProperty(String key) {
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
