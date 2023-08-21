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
package org.neo4j.server.http.cypher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.server.http.cypher.entity.HttpNode;
import org.neo4j.server.http.cypher.entity.HttpPath;
import org.neo4j.server.http.cypher.entity.HttpRelationship;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

public class CachingWriter extends BaseToObjectValueWriter<IOException> {
    private Object cachedObject;
    private ValueMapper mapper;

    private BiFunction<Long, Boolean, Optional<Node>> getNodeById;

    public CachingWriter(ValueMapper mapper) {
        this.mapper = mapper;
        this.getNodeById = (ignoredA, ignoredB) -> Optional.empty();
    }

    public Object getCachedObject() {
        return cachedObject;
    }

    @Override
    public void writeRelationship(
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted) {
        cachedObject = new HttpRelationship(
                elementId,
                relId,
                startNodeElementId,
                startNodeId,
                endNodeElementId,
                endNodeId,
                type.stringValue(),
                processProperties(properties),
                isDeleted,
                getNodeById);
    }

    @Override
    public void writePath(NodeValue[] nodes, RelationshipValue[] relationships) {
        var nodeList = convertNodeValues(nodes);
        var relList = convertRelationshipValues(relationships);

        cachedObject = new HttpPath(nodeList, relList);
    }

    @Override
    public void writeNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted) {
        var labelList = Arrays.stream((String[]) labels.asObjectCopy())
                .map(Label::label)
                .collect(Collectors.toList());
        cachedObject = new HttpNode(elementId, nodeId, labelList, processProperties(properties), isDeleted);
    }

    @Override
    protected Node newNodeEntityById(long id) {
        throw new UnsupportedOperationException("Only can write existing nodes");
    }

    @Override
    protected Node newNodeEntityByElementId(String elementId) {
        throw new UnsupportedOperationException("Only can write existing nodes");
    }

    @Override
    protected Relationship newRelationshipEntityById(long id) {
        throw new UnsupportedOperationException("Only can write existing relationships");
    }

    @Override
    protected Relationship newRelationshipEntityByElementId(String elementId) {
        throw new UnsupportedOperationException("Only can write existing relationships");
    }

    @Override
    protected Point newPoint(CoordinateReferenceSystem crs, double[] coordinate) {
        throw new UnsupportedOperationException("Does not write points");
    }

    public void setGetNodeById(BiFunction<Long, Boolean, Optional<Node>> getNodeById) {
        this.getNodeById = getNodeById;
    }

    private Map<String, Object> processProperties(MapValue properties) {
        var propertyMap = new HashMap<String, Object>();
        properties.foreach((k, v) -> {
            propertyMap.put(k, v.map(mapper));
        });
        return propertyMap;
    }

    private List<Node> convertNodeValues(NodeValue[] nodeValues) {
        var nodeArrayList = new ArrayList<Node>();
        for (NodeValue nodeValue : nodeValues) {
            writeNode(
                    nodeValue.elementId(),
                    nodeValue.id(),
                    nodeValue.labels(),
                    nodeValue.properties(),
                    nodeValue.isDeleted());
            nodeArrayList.add((HttpNode) cachedObject);
        }
        return nodeArrayList;
    }

    private List<Relationship> convertRelationshipValues(RelationshipValue[] relationships) {
        var relArrayList = new ArrayList<Relationship>();
        for (RelationshipValue relationship : relationships) {
            writeRelationship(
                    relationship.elementId(),
                    relationship.id(),
                    relationship.startNodeElementId(),
                    relationship.startNodeId(),
                    relationship.endNodeElementId(),
                    relationship.endNodeId(),
                    relationship.type(),
                    relationship.properties(),
                    relationship.isDeleted());
            relArrayList.add((HttpRelationship) cachedObject);
        }
        return relArrayList;
    }
}
