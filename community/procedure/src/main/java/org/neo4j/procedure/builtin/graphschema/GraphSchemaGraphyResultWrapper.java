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
package org.neo4j.procedure.builtin.graphschema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.procedure.builtin.graphschema.GraphSchema.NodeObjectType;
import org.neo4j.procedure.builtin.graphschema.GraphSchema.Ref;
import org.neo4j.procedure.builtin.graphschema.GraphSchema.RelationshipObjectType;
import org.neo4j.procedure.builtin.graphschema.GraphSchema.Token;

/**
 * A graph-y result build from a {@link GraphSchema}. The wrapper is needed to make the Neo4j embedded API happy.
 */
public final class GraphSchemaGraphyResultWrapper {

    public static GraphSchemaGraphyResultWrapper flat(GraphSchema graphSchema) {

        var nodeLabels = graphSchema.nodeLabels().values().stream().collect(Collectors.toMap(Token::id, Token::value));
        var relationshipTypes =
                graphSchema.relationshipTypes().values().stream().collect(Collectors.toMap(Token::id, Token::value));

        var result = new GraphSchemaGraphyResultWrapper();

        var nodeObjectTypeNodes = new HashMap<Ref, Node>();
        for (NodeObjectType nodeObjectType : graphSchema.nodeObjectTypes().values()) {
            var labels = nodeObjectType.labels().stream()
                    .map(v -> nodeLabels.get(v.value()))
                    .toArray(String[]::new);
            var node = new VirtualNode(
                    Map.of(
                            "$id",
                            nodeObjectType.id(),
                            "name",
                            labels.length == 0 ? "n/a" : labels[0],
                            "properties",
                            GraphSchemaModule.asJsonString(nodeObjectType.properties())),
                    labels);
            nodeObjectTypeNodes.put(new Ref(nodeObjectType.id()), node);
        }
        result.nodes.addAll(nodeObjectTypeNodes.values());

        for (RelationshipObjectType relationshipObjectType :
                graphSchema.relationshipObjectTypes().values()) {
            var relationship = new VirtualRelationship(
                    nodeObjectTypeNodes.get(relationshipObjectType.from()),
                    relationshipTypes.get(relationshipObjectType.type().value()),
                    Map.of(
                            "$id",
                            relationshipObjectType.id(),
                            "properties",
                            GraphSchemaModule.asJsonString(relationshipObjectType.properties())),
                    nodeObjectTypeNodes.get(relationshipObjectType.to()));
            result.relationships.add(relationship);
        }

        return result;
    }

    public static GraphSchemaGraphyResultWrapper full(GraphSchema graphSchema) {

        var nodeLabelNodes = graphSchema.nodeLabels().values().stream()
                .collect(Collectors.toMap(Token::id, t -> toVirtualNode(t, "NodeLabel")));
        var relationshipTypeNodes = graphSchema.relationshipTypes().values().stream()
                .collect(Collectors.toMap(Token::id, t -> toVirtualNode(t, "RelationshipType")));

        var result = new GraphSchemaGraphyResultWrapper();

        var nodeObjectTypeNodes = new HashMap<Ref, Node>();
        for (var entry : graphSchema.nodeObjectTypes().entrySet()) {
            var nodeObjectType = entry.getValue();
            var node = new VirtualNode(
                    Map.of(
                            "$id",
                            nodeObjectType.id(),
                            "properties",
                            GraphSchemaModule.asJsonString(nodeObjectType.properties())),
                    "NodeObjectType");
            nodeObjectTypeNodes.put(entry.getKey(), node);

            for (var ref : nodeObjectType.labels()) {
                var tokenNode = nodeLabelNodes.get(ref.value());
                result.relationships.add(new VirtualRelationship(node, "HAS_LABEL", tokenNode));
            }
        }

        for (RelationshipObjectType relationshipObjectType :
                graphSchema.relationshipObjectTypes().values()) {
            var node = new VirtualNode(
                    Map.of(
                            "$id",
                            relationshipObjectType.id(),
                            "properties",
                            GraphSchemaModule.asJsonString(relationshipObjectType.properties())),
                    "RelationshipObjectTypes");
            result.nodes.add(node);
            result.relationships.add(new VirtualRelationship(
                    node,
                    "HAS_TYPE",
                    relationshipTypeNodes.get(relationshipObjectType.type().value())));
            result.relationships.add(
                    new VirtualRelationship(node, "FROM", nodeObjectTypeNodes.get(relationshipObjectType.from())));
            result.relationships.add(
                    new VirtualRelationship(node, "TO", nodeObjectTypeNodes.get(relationshipObjectType.to())));
        }

        // Add remaining things
        result.nodes.addAll(nodeLabelNodes.values());
        result.nodes.addAll(relationshipTypeNodes.values());
        result.nodes.addAll(nodeObjectTypeNodes.values());

        return result;
    }

    private static Node toVirtualNode(Token token, String label) {

        return new VirtualNode(Map.of("$id", token.id(), "value", token.value()), "Token", label);
    }

    // Public field required for Neo4j internal API.
    public final List<Node> nodes = new ArrayList<>();
    public final List<Relationship> relationships = new ArrayList<>();

    /**
     * A virtual entity, spotting a negative ID and a random element id.
     */
    abstract static class VirtualEntity implements Entity {

        private static final Supplier<Long> ID_FACTORY = new AtomicLong(-1)::decrementAndGet;

        private final long id;

        private final String elementId;

        private final Map<String, Object> properties;

        VirtualEntity(Map<String, Object> properties) {
            this.id = ID_FACTORY.get();
            this.elementId = String.valueOf(this.id);
            this.properties = Map.copyOf(properties);
        }

        @SuppressWarnings("removal")
        @Override
        public final long getId() {
            return id;
        }

        @Override
        public final String getElementId() {
            return elementId;
        }

        @Override
        public final boolean hasProperty(String key) {
            return properties.containsKey(key);
        }

        @Override
        public final Object getProperty(String key) {
            return properties.get(key);
        }

        @Override
        public final Object getProperty(String key, Object defaultValue) {
            return properties.getOrDefault(key, defaultValue);
        }

        @Override
        public final void setProperty(String key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final Object removeProperty(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final Iterable<String> getPropertyKeys() {
            return properties.keySet();
        }

        @Override
        public final Map<String, Object> getProperties(String... keys) {
            Map<String, Object> result = new HashMap<>();
            for (String key : keys) {
                if (!hasProperty(key)) {
                    continue;
                }
                result.put(key, getProperty(key));
            }
            return result;
        }

        @Override
        public final Map<String, Object> getAllProperties() {
            return properties;
        }

        @Override
        public final void delete() {
            throw new UnsupportedOperationException();
        }
    }

    static final class VirtualNode extends VirtualEntity implements Node {

        private final Set<Label> labels;

        VirtualNode(Map<String, Object> properties, String... labels) {
            super(properties);
            this.labels = Arrays.stream(labels)
                    .map(Label::label)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(), Set::copyOf));
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
        public boolean hasRelationship(RelationshipType... types) {
            return false;
        }

        @Override
        public boolean hasRelationship(Direction direction, RelationshipType... types) {
            return false;
        }

        @Override
        public ResourceIterable<Relationship> getRelationships(Direction dir) {
            return null;
        }

        @Override
        public boolean hasRelationship(Direction dir) {
            return false;
        }

        @Override
        public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
            return null;
        }

        @Override
        public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
            throw new UnsupportedOperationException();
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
        public int getDegree(Direction direction) {
            return 0;
        }

        @Override
        public int getDegree(RelationshipType type, Direction direction) {
            return 0;
        }

        @Override
        public void addLabel(Label label) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeLabel(Label label) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasLabel(Label label) {
            return labels.contains(label);
        }

        @Override
        public Iterable<Label> getLabels() {
            return labels;
        }
    }

    static final class VirtualRelationship extends VirtualEntity implements Relationship {

        private final Node startNode;

        private final Node endNode;

        private final RelationshipType type;

        VirtualRelationship(Node startNode, String type, Node endNode) {
            this(startNode, type, Map.of(), endNode);
        }

        VirtualRelationship(Node startNode, String type, Map<String, Object> properties, Node endNode) {
            super(properties);
            this.startNode = startNode;
            this.type = RelationshipType.withName(type);
            this.endNode = endNode;
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
        public Node getOtherNode(Node node) {
            return startNode == node ? endNode : startNode;
        }

        @Override
        public Node[] getNodes() {
            return new Node[] {startNode, endNode};
        }

        @Override
        public RelationshipType getType() {
            return this.type;
        }

        @Override
        public boolean isType(RelationshipType typeInQuestion) {
            return this.type.equals(typeInQuestion);
        }
    }
}
