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
package org.neo4j.server.http.cypher.format.output.json;

import static org.neo4j.server.http.cypher.entity.Predicates.isDeleted;
import static org.neo4j.server.http.cypher.entity.Predicates.isFullNode;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.IterableWrapper;
import org.neo4j.server.http.cypher.format.api.RecordEvent;

class GraphExtractionWriter implements ResultDataContentWriter {
    @Override
    public void write(JsonGenerator out, RecordEvent recordEvent) throws IOException {
        var nodesMap = new HashMap<Long, Node>();
        var relationshipList = new ArrayList<Relationship>();

        extract(nodesMap, relationshipList, map(recordEvent));

        Set<Node> nodes = new HashSet<>(nodesMap.values());
        Set<Relationship> relationships = new HashSet<>(relationshipList);

        out.writeObjectFieldStart("graph");
        try {
            writeNodes(out, nodes);
            writeRelationships(out, relationships);
        } finally {
            out.writeEndObject();
        }
    }

    private static void writeNodes(JsonGenerator out, Iterable<Node> nodes) throws IOException {
        out.writeArrayFieldStart("nodes");
        try {
            for (Node node : nodes) {
                out.writeStartObject();
                try {
                    long nodeId = node.getId();
                    out.writeStringField("id", Long.toString(nodeId));
                    out.writeStringField("elementId", node.getElementId());
                    if (isDeleted(node)) {
                        markDeleted(out);
                    } else {
                        out.writeArrayFieldStart("labels");
                        try {
                            for (Label label : node.getLabels()) {
                                out.writeString(label.name());
                            }
                        } finally {
                            out.writeEndArray();
                        }
                        writeProperties(out, node);
                    }
                } finally {
                    out.writeEndObject();
                }
            }
        } finally {
            out.writeEndArray();
        }
    }

    private static void markDeleted(JsonGenerator out) throws IOException {
        out.writeBooleanField("deleted", Boolean.TRUE);
    }

    private static void writeRelationships(JsonGenerator out, Iterable<Relationship> relationships) throws IOException {
        out.writeArrayFieldStart("relationships");
        try {
            for (Relationship relationship : relationships) {
                out.writeStartObject();
                try {
                    long relationshipId = relationship.getId();
                    out.writeStringField("id", Long.toString(relationshipId));
                    out.writeStringField("elementId", relationship.getElementId());
                    if (isDeleted(relationship)) {
                        markDeleted(out);
                    } else {
                        out.writeStringField("type", relationship.getType().name());
                        out.writeStringField("startNode", Long.toString(relationship.getStartNodeId()));
                        out.writeStringField(
                                "startNodeElementId",
                                relationship.getStartNode().getElementId());
                        out.writeStringField("endNode", Long.toString(relationship.getEndNodeId()));
                        out.writeStringField(
                                "endNodeElementId", relationship.getEndNode().getElementId());
                        writeProperties(out, relationship);
                    }
                } finally {
                    out.writeEndObject();
                }
            }
        } finally {
            out.writeEndArray();
        }
    }

    private static void writeProperties(JsonGenerator out, Entity container) throws IOException {
        out.writeObjectFieldStart("properties");
        try {
            for (Map.Entry<String, Object> property :
                    container.getAllProperties().entrySet()) {
                out.writeObjectField(property.getKey(), property.getValue());
            }
        } finally {
            out.writeEndObject();
        }
    }

    private static void extract(Map<Long, Node> nodes, ArrayList<Relationship> relationships, Iterable<?> source)
            throws IOException {
        for (Object item : source) {
            if (item instanceof Node node) {
                addNode(nodes, node.getId(), () -> node);
            } else if (item instanceof Relationship relationship) {
                relationships.add(relationship);
                addNode(nodes, relationship.getStartNodeId(), relationship::getStartNode);
                addNode(nodes, relationship.getEndNodeId(), relationship::getEndNode);
            }
            if (item instanceof Path path) {
                for (Node node : path.nodes()) {
                    addNode(nodes, node.getId(), () -> node);
                }
                for (Relationship relationship : path.relationships()) {
                    relationships.add(relationship);
                }
            } else if (item instanceof Map<?, ?>) {
                extract(nodes, relationships, ((Map<?, ?>) item).values());
            } else if (item instanceof Iterable<?>) {
                extract(nodes, relationships, (Iterable<?>) item);
            }
        }
    }

    private static void addNode(Map<Long, Node> nodes, Long id, Supplier<Node> nodeSupplier) throws IOException {
        if (nodes.containsKey(id)) {
            Node existingNode = nodes.get(id);

            if (!isDeleted(existingNode) && !isFullNode(existingNode)) {
                nodes.remove(id);
                nodes.put(id, nodeSupplier.get());
            }
        } else {
            nodes.put(id, nodeSupplier.get());
        }
    }

    private static Iterable<?> map(RecordEvent recordEvent) {
        return new IterableWrapper<>(recordEvent.getColumns()) {
            @Override
            protected Object underlyingObjectToObject(String key) {
                return recordEvent.getValue(key);
            }
        };
    }
}
