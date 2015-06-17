/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
        Transaction transaction = graph.beginTx();

        try
        {
            NodeRepresentation nodeRepresentation = super.createNode( properties, labels );
            transaction.success();
            return nodeRepresentation;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public NodeRepresentation getNode( long nodeId ) throws NodeNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            NodeRepresentation node = super.getNode( nodeId );
            transaction.success();
            return node;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void deleteNode( long nodeId ) throws NodeNotFoundException, ConstraintViolationException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.deleteNode( nodeId );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void setNodeProperty( long nodeId, String key, Object value ) throws PropertyValueException,
            NodeNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.setNodeProperty( nodeId, key, value );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void removeNodeProperty( long nodeId, String key ) throws NodeNotFoundException, NoSuchPropertyException
    {
        Transaction transaction = graph.beginTx();
        try
        {
            super.removeNodeProperty( nodeId, key );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void setAllNodeProperties( long nodeId, Map<String, Object> properties ) throws PropertyValueException,
            NodeNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.setAllNodeProperties( nodeId, properties );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void removeAllNodeProperties( long nodeId ) throws NodeNotFoundException, PropertyValueException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.removeAllNodeProperties( nodeId );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void addLabelToNode( long nodeId, Collection<String> labelNames ) throws NodeNotFoundException,
            BadInputException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.addLabelToNode( nodeId, labelNames );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void removeLabelFromNode( long nodeId, String labelName ) throws NodeNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.removeLabelFromNode( nodeId, labelName );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public IndexRepresentation createNodeIndex( Map<String, Object> indexSpecification )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            IndexRepresentation indexRepresentation = super.createNodeIndex( indexSpecification );
            transaction.success();
            return indexRepresentation;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public RelationshipRepresentation createRelationship( long startNodeId, long endNodeId, String type, Map<String,
            Object> properties ) throws StartNodeNotFoundException, EndNodeNotFoundException, PropertyValueException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            RelationshipRepresentation relationshipRepresentation = super.createRelationship( startNodeId, endNodeId,
                    type,
                    properties );
            transaction.success();
            return relationshipRepresentation;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public RelationshipRepresentation getRelationship( long relationshipId ) throws RelationshipNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            RelationshipRepresentation relationship = super.getRelationship( relationshipId );
            transaction.success();
            return relationship;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void deleteRelationship( long relationshipId ) throws RelationshipNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.deleteRelationship( relationshipId );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public ListRepresentation getNodeRelationships( long nodeId, RelationshipDirection direction, Collection<String>
            types ) throws NodeNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            ListRepresentation nodeRelationships = super.getNodeRelationships( nodeId, direction, types );
            transaction.success();
            return nodeRelationships;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void setAllRelationshipProperties( long relationshipId, Map<String, Object> properties ) throws
            PropertyValueException, RelationshipNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.setAllRelationshipProperties( relationshipId, properties );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void setRelationshipProperty( long relationshipId, String key, Object value ) throws
            PropertyValueException, RelationshipNotFoundException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.setRelationshipProperty( relationshipId, key, value );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void removeAllRelationshipProperties( long relationshipId ) throws RelationshipNotFoundException,
            PropertyValueException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.removeAllRelationshipProperties( relationshipId );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void removeRelationshipProperty( long relationshipId, String key ) throws RelationshipNotFoundException,
            NoSuchPropertyException
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.removeRelationshipProperty( relationshipId, key );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public IndexedEntityRepresentation addToNodeIndex( String indexName, String key, String value, long nodeId )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            IndexedEntityRepresentation indexedEntityRepresentation = super.addToNodeIndex( indexName, key, value,
                    nodeId );
            transaction.success();
            return indexedEntityRepresentation;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void removeFromNodeIndex( String indexName, String key, String value, long id )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.removeFromNodeIndex( indexName, key, value, id );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void removeFromNodeIndexNoValue( String indexName, String key, long id )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.removeFromNodeIndexNoValue( indexName, key, id );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public void removeFromNodeIndexNoKeyValue( String indexName, long id )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            super.removeFromNodeIndexNoKeyValue( indexName, id );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public PathRepresentation findSinglePath( long startId, long endId, Map<String, Object> map )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            PathRepresentation singlePath = super.findSinglePath( startId, endId, map );
            transaction.success();
            return singlePath;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public ListRepresentation getNodesWithLabel( String labelName, Map<String, Object> properties )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            ListRepresentation nodesWithLabel = super.getNodesWithLabel( labelName, properties );
            transaction.success();
            return nodesWithLabel;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public IndexDefinitionRepresentation createSchemaIndex( String labelName, Iterable<String> propertyKey )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            IndexDefinitionRepresentation indexDefinitionRepresentation = super.createSchemaIndex( labelName,
                    propertyKey );
            transaction.success();
            return indexDefinitionRepresentation;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public boolean dropSchemaIndex( String labelName, String propertyKey )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            boolean result = super.dropSchemaIndex( labelName, propertyKey );
            transaction.success();
            return result;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public ConstraintDefinitionRepresentation createPropertyUniquenessConstraint( String labelName,
                                                                                  Iterable<String> propertyKeys )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            ConstraintDefinitionRepresentation constraintDefinitionRepresentation = super
                    .createPropertyUniquenessConstraint( labelName, propertyKeys );
            transaction.success();
            return constraintDefinitionRepresentation;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public boolean dropPropertyUniquenessConstraint( String labelName, Iterable<String> propertyKeys )
    {
        Transaction transaction = graph.beginTx();

        try
        {
            boolean result = super.dropPropertyUniquenessConstraint( labelName, propertyKeys );
            transaction.success();
            return result;
        }
        finally
        {
            transaction.finish();
        }
    }
}
