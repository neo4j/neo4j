/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * A convenient class for testing RestfulGraphDatabase: just wraps every "web call" in a transaction like the
 * TransactionFilter would have if deployed in a jax-rs container
 */
public class TransactionWrappingRestfulGraphDatabase extends RestfulGraphDatabase
{
    private final GraphDatabaseService graph;
    private final RestfulGraphDatabase restfulGraphDatabase;

    public TransactionWrappingRestfulGraphDatabase( GraphDatabaseService graph,
                                                    RestfulGraphDatabase restfulGraphDatabase )
    {
        super( null, null, null, null );

        this.graph = graph;
        this.restfulGraphDatabase = restfulGraphDatabase;
    }

    @Override
    public Response createRelationship( long startNodeId, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.createRelationship( startNodeId, body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response getNodeRelationships( long nodeId, DatabaseActions.RelationshipDirection direction,
                                          AmpersandSeparatedCollection types )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.getNodeRelationships( nodeId, direction, types );
            return response;
        }
    }

    @Override
    public Response deleteAllNodeProperties( long nodeId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.deleteAllNodeProperties( nodeId );
            transaction.success();
            return response;
        }
    }

    @Override
    public Response getAllNodeProperties( long nodeId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.getAllNodeProperties( nodeId );
            return response;
        }
    }

    @Override
    public Response createNode( String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.createNode( body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response deleteAllRelationshipProperties( long relationshipId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.deleteAllRelationshipProperties( relationshipId );
            transaction.success();
            return response;
        }
    }

    @Override
    public Response setRelationshipProperty( long relationshipId, String key, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.setRelationshipProperty( relationshipId, key, body );
            transaction.success();
            return response;
        }
    }

    @Override
    public Response getSchemaConstraintsForLabelAndPropertyUniqueness( String labelName,
                                                                       AmpersandSeparatedCollection propertyKeys )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response getSchemaConstraintsForLabelAndUniqueness( String labelName )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response getSchemaConstraintsForLabel( String labelName )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response getSchemaConstraints()
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response dropPropertyUniquenessConstraint( String labelName,
                                                      AmpersandSeparatedCollection properties )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response createPropertyUniquenessConstraint( String labelName, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.createPropertyUniquenessConstraint( labelName, body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response getSchemaIndexesForLabel( String labelName )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response dropSchemaIndex( String labelName,
                                     AmpersandSeparatedCollection properties )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response createSchemaIndex( String labelName, String body )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response allPaths( long startNode, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.allPaths( startNode, body );
        }
    }

    @Override
    public Response singlePath( long startNode, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.singlePath( startNode, body );
        }
    }

    @Override
    public Response deleteRelationshipProperty( long relationshipId, String key )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.deleteRelationshipProperty( relationshipId, key );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response setAllRelationshipProperties( long relationshipId, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.setAllRelationshipProperties( relationshipId, body );
            transaction.success();
            return response;
        }
    }

    @Override
    public Response getRelationshipProperty( long relationshipId,
                                             String key )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.getRelationshipProperty( relationshipId, key );
        }
    }

    @Override
    public Response getAllRelationshipProperties( long relationshipId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.getAllRelationshipProperties( relationshipId );
        }
    }

    @Override
    public Response getNodeRelationships( long nodeId, DatabaseActions.RelationshipDirection direction )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response deleteRelationship( long relationshipId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.deleteRelationship( relationshipId );
            transaction.success();
            return response;
        }
    }

    @Override
    public Response getRelationship( long relationshipId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.getRelationship( relationshipId );
        }
    }

    @Override
    public Response getAllLabels( boolean inUse )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.getAllLabels( inUse );
        }
    }

    @Override
    public Response getNodesWithLabelAndProperty( String labelName, UriInfo uriInfo )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response getNodeLabels( long nodeId )
    {
        try ( Transaction ignored = graph.beginTx() )
        {
            return restfulGraphDatabase.getNodeLabels( nodeId );
        }
    }

    @Override
    public Response removeNodeLabel( long
            nodeId, String labelName )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response setNodeLabels( long nodeId, String body )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response addNodeLabel( long nodeId, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.addNodeLabel( nodeId, body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response deleteNodeProperty( long nodeId, String key )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.deleteNodeProperty( nodeId, key );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response getNodeProperty( long
            nodeId, String key )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.getNodeProperty( nodeId, key );
        }
    }

    @Override
    public Response setNodeProperty( long
            nodeId, String key, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.setNodeProperty( nodeId, key, body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response setAllNodeProperties( long nodeId, String body )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.setAllNodeProperties( nodeId, body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response deleteNode( long nodeId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            Response response = restfulGraphDatabase.deleteNode( nodeId );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response getNode( long nodeId )
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.getNode( nodeId );
        }
    }

    @Override
    public Response getRoot()
    {
        try ( Transaction transaction = graph.beginTx() )
        {
            return restfulGraphDatabase.getRoot();
        }
    }
}
