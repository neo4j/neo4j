/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.rest.domain;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;

public class StorageActions {

    private final URI baseUri;
    private final Database graphdb;

    public StorageActions(URI baseUri, Database theDatbase) throws DatabaseBlockedException {
        this.baseUri = baseUri;
        this.graphdb = theDatbase;
    }

    public NodeRepresentation getReferenceNode() throws DatabaseBlockedException {
        Transaction tx = graphdb.graph.beginTx();
        try {
            NodeRepresentation result = retrieveNode(graphdb.graph.getReferenceNode().getId());
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public NodeRepresentation createNode(PropertiesMap properties) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.createNode();
            properties.storeTo(node);
            tx.success();
            return new NodeRepresentation(baseUri, node);
        } finally {
            tx.finish();
        }
    }

    public void setNodeProperties(long nodeId, PropertiesMap properties) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.getNodeById(nodeId);
            deleteProperties(node);
            properties.storeTo(node);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    private void deleteProperties(PropertyContainer container) {
        for (String key : container.getPropertyKeys()) {
            container.removeProperty(key);
        }
    }

    public NodeRepresentation retrieveNode(long nodeId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.getNodeById(nodeId);
            tx.success();
            return new NodeRepresentation(baseUri, node);
        } finally {
            tx.finish();
        }
    }

    public PropertiesMap getNodeProperties(long nodeId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            PropertiesMap result = new PropertiesMap(graphdb.graph.getNodeById(nodeId));
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public void deleteNode(long nodeId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.getNodeById(nodeId);
            node.delete();
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public void setNodeProperty(long nodeId, String key, Object value) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.getNodeById(nodeId);
            node.setProperty(key, value);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public Object getNodeProperty(long nodeId, String key) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.getNodeById(nodeId);
            Object value = node.getProperty(key);
            tx.success();
            return value;
        } finally {
            tx.finish();
        }
    }

    public RelationshipRepresentation createRelationship(String type, long startNodeId, long endNodeId, PropertiesMap properties)
            throws StartNodeNotFoundException, EndNodeNotFoundException, StartNodeSameAsEndNodeException, DatabaseBlockedException {

        if (startNodeId == endNodeId) {
            throw new StartNodeSameAsEndNodeException();
        }
        Transaction tx = graphdb.graph.beginTx();
        try {
            Node startNode;
            Node endNode;
            try {
                startNode = graphdb.graph.getNodeById(startNodeId);
            } catch (NotFoundException e) {
                throw new StartNodeNotFoundException();
            }
            try {
                endNode = graphdb.graph.getNodeById(endNodeId);
            } catch (NotFoundException e) {
                throw new EndNodeNotFoundException();
            }
            Relationship relationship = startNode.createRelationshipTo(endNode, DynamicRelationshipType.withName(type));
            properties.storeTo(relationship);
            RelationshipRepresentation result = new RelationshipRepresentation(baseUri, relationship);
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public void removeNodeProperties(long nodeId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.getNodeById(nodeId);
            deleteProperties(node);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public boolean removeNodeProperty(long nodeId, String key) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.getNodeById(nodeId);
            boolean removed = node.removeProperty(key) != null;
            tx.success();
            return removed;
        } finally {
            tx.finish();
        }
    }

    public RelationshipRepresentation retrieveRelationship(long relationshipId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship relationship = graphdb.graph.getRelationshipById(relationshipId);
            tx.success();
            return new RelationshipRepresentation(baseUri, relationship);
        } finally {
            tx.finish();
        }
    }

    public PropertiesMap getRelationshipProperties(long relationshipId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            PropertiesMap result = new PropertiesMap(graphdb.graph.getRelationshipById(relationshipId));
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public Object getRelationshipProperty(long relationshipId, String key) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship relationship = graphdb.graph.getRelationshipById(relationshipId);
            Object result = relationship.getProperty(key);
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public void removeRelationship(long relationshipId) throws DatabaseBlockedException {
        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship relationship = graphdb.graph.getRelationshipById(relationshipId);
            relationship.delete();
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public List<RelationshipRepresentation> retrieveRelationships(long nodeId, RelationshipDirection direction, String... typeNames)
            throws DatabaseBlockedException {

        RelationshipType[] types = new RelationshipType[typeNames.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = DynamicRelationshipType.withName(typeNames[i]);
        }
        Transaction tx = graphdb.graph.beginTx();
        try {
            final Node node = graphdb.graph.getNodeById(nodeId);
            List<RelationshipRepresentation> result = new LinkedList<RelationshipRepresentation>();
            if (types.length > 1) {
                for (Relationship rel : node.getRelationships(types)) {
                    switch (direction) {
                    case in:
                        if (!rel.getEndNode().equals(node))
                            continue;
                        break;
                    case out:
                        if (!rel.getStartNode().equals(node))
                            continue;
                        break;
                    }
                    result.add(new RelationshipRepresentation(baseUri, rel));
                }
            } else {
                Iterable<Relationship> relationships;
                if (types.length == 0) {
                    relationships = node.getRelationships(direction.internal);
                } else {
                    relationships = node.getRelationships(types[0], direction.internal);
                }
                for (Relationship rel : relationships) {
                    result.add(new RelationshipRepresentation(baseUri, rel));
                }
            }
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public void setRelationshipProperties(long relationshipId, PropertiesMap properties) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship relationship = graphdb.graph.getRelationshipById(relationshipId);
            deleteProperties(relationship);
            properties.storeTo(relationship);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public void setRelationshipProperty(long relationshipId, String key, Object value) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship relationship = graphdb.graph.getRelationshipById(relationshipId);
            relationship.setProperty(key, value);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public void removeRelationshipProperties(long relationshipId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship relationship = graphdb.graph.getRelationshipById(relationshipId);
            deleteProperties(relationship);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public boolean removeRelationshipProperty(long relationshipId, String propertyKey) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship relationship = graphdb.graph.getRelationshipById(relationshipId);
            boolean removed = relationship.removeProperty(propertyKey) != null;
            tx.success();
            return removed;
        } finally {
            tx.finish();
        }
    }

    public IndexedRepresentation addNodeToIndex(String indexName, String key, Object value, long nodeId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Node node = graphdb.graph.getNodeById(nodeId);
            Index<Node> index = graphdb.getNodeIndex(indexName);
            index.add( node, key, value );
            tx.success();
            return new IndexedRepresentation(baseUri, IndexedRepresentation.NODE, indexName, key, value, node.getId());
        } finally {
            tx.finish();
        }
    }
    
    public IndexedRepresentation addRelationshipToIndex(String indexName, String key, Object value, long nodeId) throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship relationship = graphdb.graph.getRelationshipById( nodeId );
            Index<Relationship> index = graphdb.getRelationshipIndex( indexName );
            index.add( relationship, key, value );
            tx.success();
            return new IndexedRepresentation(baseUri,IndexedRepresentation.RELATIONSHIP, indexName, key, value, relationship.getId());
        } finally {
            tx.finish();
        }
    }

    public boolean relationshipIsIndexed(String indexName, String key, Object value, long relationshipId) throws DatabaseBlockedException {

        Index<Relationship> index = graphdb.getRelationshipIndex( indexName );
        Transaction tx = graphdb.graph.beginTx();
        try {
            Relationship expectedNode = graphdb.graph.getRelationshipById( relationshipId );
            IndexHits<Relationship> hits = index.get( key, value );
            boolean contains = iterableContains(hits, expectedNode);
            tx.success();
            return contains;
        } finally {
            tx.finish();
        }
    }

    public boolean nodeIsIndexed(String indexName, String key, Object value, long nodeId) throws DatabaseBlockedException {

        Index<Node> index = graphdb.getNodeIndex( indexName );
        Transaction tx = graphdb.graph.beginTx();
        try {
            Node expectedNode = graphdb.graph.getNodeById(nodeId);
            IndexHits<Node> hits = index.get( key, value );
            boolean contains = iterableContains( hits, expectedNode );
            tx.success();
            return contains;
        } finally {
            tx.finish();
        }
    }

    private <T> boolean iterableContains(Iterable<T> iterable, T expectedElement )
    {
        for(T possibleMatch : iterable) {
            if (possibleMatch.equals( expectedElement )) return true;
        }
        return false;
    }


    public List<IndexedRelationshipRepresentation> getIndexedRelationships(String indexName, String key, Object value) throws DatabaseBlockedException {

        Index<Relationship> index = graphdb.getRelationshipIndex( indexName );
        
        Transaction tx = graphdb.graph.beginTx();
        try {
            List<IndexedRelationshipRepresentation> result = new ArrayList<IndexedRelationshipRepresentation>();
            for (Relationship relationship : index.get(key, value)) {
                result.add(new IndexedRelationshipRepresentation(baseUri, relationship, new IndexedRepresentation(baseUri, IndexedRepresentation.RELATIONSHIP, indexName, key, value, relationship.getId())));
            }
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }
    
    public List<IndexedNodeRepresentation> getIndexedNodes(String indexName, String key, Object value) throws DatabaseBlockedException {

        Index<Node> index = graphdb.getNodeIndex( indexName );
        
        Transaction tx = graphdb.graph.beginTx();
        try {
            List<IndexedNodeRepresentation> result = new ArrayList<IndexedNodeRepresentation>();
            for (Node node : index.get(key, value)) {
                result.add(new IndexedNodeRepresentation(baseUri, node, new IndexedRepresentation(baseUri, IndexedRepresentation.NODE, indexName, key, value, node.getId())));
            }
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public boolean removeNodeFromIndex(String indexName, String key, Object value, long nodeId) throws DatabaseBlockedException {

        Index<Node> index = graphdb.getNodeIndex( indexName );
        Transaction tx = graphdb.graph.beginTx();
        try {
            index.remove(graphdb.graph.getNodeById(nodeId), key, value);
            tx.success();
            return true;
        } finally {
            tx.finish();
        }
    }

    public boolean removeRelationshipFromIndex(String indexName, String key, Object value, long nodeId) throws DatabaseBlockedException {

        Index<Relationship> index = graphdb.getRelationshipIndex( indexName );
        Transaction tx = graphdb.graph.beginTx();
        try {
            index.remove(graphdb.graph.getRelationshipById(nodeId), key, value);
            tx.success();
            return true;
        } finally {
            tx.finish();
        }
    }

    public List<Representation> traverseAndCollect(long startNode, Map<String, Object> description, TraverserReturnType returnType)
            throws DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();
        try {
            List<Representation> result = new ArrayList<Representation>();
            TraversalDescription traversalDescription = TraversalDescriptionBuilder.from(description);
            for (Path position : traversalDescription.traverse(graphdb.graph.getNodeById(startNode))) {
                result.add(returnType.toRepresentation(baseUri, position));
            }
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public static enum TraverserReturnType {
        node {
            @Override
            public Representation toRepresentation(URI baseUri, Path position) {
                return new NodeRepresentation(baseUri, position.endNode());
            }
        },
        relationship {
            @Override
            public Representation toRepresentation(URI baseUri, Path position) {
                return new RelationshipRepresentation(baseUri, position.lastRelationship());
            }
        },
        path {
            @Override
            public Representation toRepresentation(URI baseUri, Path position) {
                return new PathRepresentation(baseUri, position);
            }
        };

        abstract Representation toRepresentation(URI baseUri, Path position);
    }

    public List<PathRepresentation> findPaths(long startNodeId, long endNodeId, boolean single, Map<String, Object> map) throws StartNodeNotFoundException,
            EndNodeNotFoundException, StartNodeSameAsEndNodeException, DatabaseBlockedException {

        Transaction tx = graphdb.graph.beginTx();

        try {
            Node startNode;
            Node endNode;
            try {
                startNode = graphdb.graph.getNodeById(startNodeId);
            } catch (NotFoundException e) {
                throw new StartNodeNotFoundException();
            }
            try {
                endNode = graphdb.graph.getNodeById(endNodeId);
            } catch (NotFoundException e) {
                throw new EndNodeNotFoundException();
            }

            Integer maxDepth = (Integer) map.get("max depth");
            maxDepth = (maxDepth != null) ? maxDepth : new Integer(1);

            if (!single) {
                Boolean singleBoolean = (Boolean) map.get("single");
                if (singleBoolean != null) {
                    single = singleBoolean;
                }
            }

            String algorithm = (String) map.get("algorithm");
            algorithm = (algorithm != null) ? algorithm : "shortestPath";

            RelationshipExpander expander = RelationshipExpanderBuilder.describeRelationships(map);

            // TODO Check for unsupported algorithm name.
            // TODO Anyway to make this more dynamic so we don't have to
            // change this code every time a new algo is added to the base
            // class?
            PathFinder<Path> finder = null;
            if (algorithm.equals("shortestPath")) {
                finder = GraphAlgoFactory.shortestPath(expander, maxDepth.intValue());
            } else if (algorithm.equals("allSimplePaths")) {
                finder = GraphAlgoFactory.allSimplePaths(expander, maxDepth.intValue());
            } else if (algorithm.equals("allPaths")) {
                finder = GraphAlgoFactory.allPaths(expander, maxDepth.intValue());
            }

            List<PathRepresentation> result = new ArrayList<PathRepresentation>();
            if (finder != null) {
                if (single) {
                    Path path = finder.findSinglePath(startNode, endNode);
                    if (path != null) {
                        result.add(new PathRepresentation(baseUri, path));
                    }
                } else {
                    for (Path path : finder.findAllPaths(startNode, endNode)) {
                        result.add(new PathRepresentation(baseUri, path));
                    }
                }
            }

            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    public NodeIndexRepresentation createNodeIndex( String indexName ) {
        Index<Node> createdIndex = graphdb.getIndexManager().forNodes( indexName );
        return new NodeIndexRepresentation( baseUri, indexName, graphdb.getIndexManager().getConfiguration( createdIndex ) );
    }

    public RelationshipIndexRepresentation createRelationshipIndex( String indexName ) {
        RelationshipIndex createdIndex = graphdb.getIndexManager().forRelationships( indexName );
        return new RelationshipIndexRepresentation( baseUri, indexName, graphdb.getIndexManager().getConfiguration( createdIndex ) );
    }
}
