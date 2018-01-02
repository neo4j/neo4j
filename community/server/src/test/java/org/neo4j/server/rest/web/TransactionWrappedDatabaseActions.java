/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.web;

import java.util.Collection;
import java.util.Map;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ConstraintDefinitionRepresentation;
import org.neo4j.server.rest.repr.IndexDefinitionRepresentation;
import org.neo4j.server.rest.repr.IndexRepresentation;
import org.neo4j.server.rest.repr.IndexedEntityRepresentation;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.PathRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;

/**
 * A class that is helpful when testing DatabaseActions. The alternative would be to writ tx-scaffolding in each test.
 * <p>
 * Some methods are _not_ wrapped: those are the ones that return a representation which is later serialised,
 * as that requires a transaction. For those, the test have scaffolding added.
 */
public class TransactionWrappedDatabaseActions extends DatabaseActions
{
    private final GraphDatabaseAPI graph;

    public TransactionWrappedDatabaseActions( LeaseManager leaseManager, GraphDatabaseAPI graph )
    {
        super( leaseManager, graph );
        this.graph = graph;
    }

    @Override
    public NodeRepresentation createNode( Map<String, Object> properties, Label... labels ) throws
            PropertyValueException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            NodeRepresentation nodeRepresentation = super.createNode( properties, labels );
            transaction.success();
            return nodeRepresentation;
        }
    }

    @Override
    public NodeRepresentation getNode( long nodeId ) throws NodeNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            NodeRepresentation node = super.getNode( nodeId );
            transaction.success();
            return node;
        }
    }

    @Override
    public void deleteNode( long nodeId ) throws NodeNotFoundException, ConstraintViolationException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.deleteNode( nodeId );
            transaction.success();
        }
    }

    @Override
    public void setNodeProperty( long nodeId, String key, Object value ) throws PropertyValueException,
            NodeNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.setNodeProperty( nodeId, key, value );
            transaction.success();
        }
    }

    @Override
    public void removeNodeProperty( long nodeId, String key ) throws NodeNotFoundException, NoSuchPropertyException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.removeNodeProperty( nodeId, key );
            transaction.success();
        }
    }

    @Override
    public void setAllNodeProperties( long nodeId, Map<String, Object> properties ) throws PropertyValueException,
            NodeNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.setAllNodeProperties( nodeId, properties );
            transaction.success();
        }
    }

    @Override
    public void removeAllNodeProperties( long nodeId ) throws NodeNotFoundException, PropertyValueException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.removeAllNodeProperties( nodeId );
            transaction.success();
        }
    }

    @Override
    public void addLabelToNode( long nodeId, Collection<String> labelNames ) throws NodeNotFoundException,
            BadInputException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.addLabelToNode( nodeId, labelNames );
            transaction.success();
        }
    }

    @Override
    public void removeLabelFromNode( long nodeId, String labelName ) throws NodeNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.removeLabelFromNode( nodeId, labelName );
            transaction.success();
        }
    }

    @Override
    public IndexRepresentation createNodeIndex( Map<String, Object> indexSpecification )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            IndexRepresentation indexRepresentation = super.createNodeIndex( indexSpecification );
            transaction.success();
            return indexRepresentation;
        }
    }

    @Override
    public RelationshipRepresentation createRelationship( long startNodeId, long endNodeId, String type, Map<String,
            Object> properties ) throws StartNodeNotFoundException, EndNodeNotFoundException, PropertyValueException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            RelationshipRepresentation relationshipRepresentation = super.createRelationship( startNodeId, endNodeId,
                    type,
                    properties );
            transaction.success();
            return relationshipRepresentation;
        }
    }

    @Override
    public RelationshipRepresentation getRelationship( long relationshipId ) throws RelationshipNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            RelationshipRepresentation relationship = super.getRelationship( relationshipId );
            transaction.success();
            return relationship;
        }
    }

    @Override
    public void deleteRelationship( long relationshipId ) throws RelationshipNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.deleteRelationship( relationshipId );
            transaction.success();
        }
    }

    @Override
    public ListRepresentation getNodeRelationships( long nodeId, RelationshipDirection direction, Collection<String>
            types ) throws NodeNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            ListRepresentation nodeRelationships = super.getNodeRelationships( nodeId, direction, types );
            transaction.success();
            return nodeRelationships;
        }
    }

    @Override
    public void setAllRelationshipProperties( long relationshipId, Map<String, Object> properties ) throws
            PropertyValueException, RelationshipNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.setAllRelationshipProperties( relationshipId, properties );
            transaction.success();
        }
    }

    @Override
    public void setRelationshipProperty( long relationshipId, String key, Object value ) throws
            PropertyValueException, RelationshipNotFoundException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.setRelationshipProperty( relationshipId, key, value );
            transaction.success();
        }
    }

    @Override
    public void removeAllRelationshipProperties( long relationshipId ) throws RelationshipNotFoundException,
            PropertyValueException
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.removeAllRelationshipProperties( relationshipId );
            transaction.success();
        }
    }

    @Override
    public void removeRelationshipProperty( long relationshipId, String key ) throws RelationshipNotFoundException,
            NoSuchPropertyException
    {

        try ( Transaction transaction = graph.beginTx() )
        {
            super.removeRelationshipProperty( relationshipId, key );
            transaction.success();
        }
    }

    @Override
    public IndexedEntityRepresentation addToNodeIndex( String indexName, String key, String value, long nodeId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            IndexedEntityRepresentation indexedEntityRepresentation = super.addToNodeIndex( indexName, key, value,
                    nodeId );
            transaction.success();
            return indexedEntityRepresentation;
        }
    }

    @Override
    public void removeFromNodeIndex( String indexName, String key, String value, long id )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.removeFromNodeIndex( indexName, key, value, id );
            transaction.success();
        }
    }

    @Override
    public void removeFromNodeIndexNoValue( String indexName, String key, long id )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.removeFromNodeIndexNoValue( indexName, key, id );
            transaction.success();
        }
    }

    @Override
    public void removeFromNodeIndexNoKeyValue( String indexName, long id )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            super.removeFromNodeIndexNoKeyValue( indexName, id );
            transaction.success();
        }
    }

    @Override
    public PathRepresentation findSinglePath( long startId, long endId, Map<String, Object> map )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            PathRepresentation singlePath = super.findSinglePath( startId, endId, map );
            transaction.success();
            return singlePath;
        }
    }

    @Override
    public ListRepresentation getNodesWithLabel( String labelName, Map<String, Object> properties )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            ListRepresentation nodesWithLabel = super.getNodesWithLabel( labelName, properties );
            transaction.success();
            return nodesWithLabel;
        }
    }

    @Override
    public IndexDefinitionRepresentation createSchemaIndex( String labelName, Iterable<String> propertyKey )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            IndexDefinitionRepresentation indexDefinitionRepresentation = super.createSchemaIndex( labelName,
                    propertyKey );
            transaction.success();
            return indexDefinitionRepresentation;
        }
    }

    @Override
    public boolean dropSchemaIndex( String labelName, String propertyKey )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            boolean result = super.dropSchemaIndex( labelName, propertyKey );
            transaction.success();
            return result;
        }
    }

    @Override
    public ConstraintDefinitionRepresentation createPropertyUniquenessConstraint( String labelName,
                                                                                  Iterable<String> propertyKeys )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            ConstraintDefinitionRepresentation constraintDefinitionRepresentation = super
                    .createPropertyUniquenessConstraint( labelName, propertyKeys );
            transaction.success();
            return constraintDefinitionRepresentation;
        }
    }

    @Override
    public boolean dropPropertyUniquenessConstraint( String labelName, Iterable<String> propertyKeys )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            boolean result = super.dropPropertyUniquenessConstraint( labelName, propertyKeys );
            transaction.success();
            return result;
        }
    }

    @Override
    public boolean dropNodePropertyExistenceConstraint( String labelName, Iterable<String> propertyKeys )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            boolean result = super.dropNodePropertyExistenceConstraint( labelName, propertyKeys );
            transaction.success();
            return result;
        }
    }

    @Override
    public boolean dropRelationshipPropertyExistenceConstraint( String typeName, Iterable<String> propertyKeys )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            boolean result = super.dropRelationshipPropertyExistenceConstraint( typeName, propertyKeys );
            transaction.success();
            return result;
        }
    }
}
