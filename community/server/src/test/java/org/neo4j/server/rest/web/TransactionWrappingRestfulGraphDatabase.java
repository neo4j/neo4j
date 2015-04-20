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

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.server.rest.domain.TraverserReturnType;

/**
 * A convenient class for testing RestfulGraphDatabase: just wraps every "web call" in a transaction like the
 * TransactionFilter would have if deployed in a jax-rs container
 */
public class TransactionWrappingRestfulGraphDatabase extends RestfulGraphDatabase
{
    private final InternalAbstractGraphDatabase graph;
    private final RestfulGraphDatabase restfulGraphDatabase;

    public TransactionWrappingRestfulGraphDatabase( InternalAbstractGraphDatabase graph,
                                                    RestfulGraphDatabase restfulGraphDatabase )
    {
        super( null, null, null );

        this.graph = graph;
        this.restfulGraphDatabase = restfulGraphDatabase;
    }

    @Override
    public Response addToNodeIndex( String indexName, String unique, String uniqueness,
                                    String postBody )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.addToNodeIndex( indexName, unique, uniqueness,
                    postBody );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response createRelationship( long startNodeId, String body )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.createRelationship( startNodeId, body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response deleteNodeIndex( String indexName )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteNodeIndex( indexName );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getNodeRelationships( long nodeId, DatabaseActions.RelationshipDirection direction,
                                          AmpersandSeparatedCollection types )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.getNodeRelationships( nodeId, direction, types );
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response deleteAllNodeProperties( long nodeId )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteAllNodeProperties( nodeId );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getAllNodeProperties( long nodeId )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.getAllNodeProperties( nodeId );
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response traverse( long startNode, TraverserReturnType returnType, String body )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.traverse( startNode, returnType, body );
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response createNode( String body )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.createNode( body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response deleteAllRelationshipProperties( long relationshipId )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteAllRelationshipProperties( relationshipId );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response addToRelationshipIndex( String indexName, String unique, String uniqueness,
                                            String postBody )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.addToRelationshipIndex( indexName, unique, uniqueness,
                    postBody );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getIndexedNodes( String indexName, String key, String value )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.getIndexedNodes( indexName, key, value );
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getRelationshipIndexRoot()
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.getRelationshipIndexRoot();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response setRelationshipProperty( long relationshipId, String key, String body )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.setRelationshipProperty( relationshipId, key, body );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
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
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.allPaths( startNode, body );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response singlePath( long startNode, String body )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.singlePath( startNode, body );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response createPagedTraverser( long startNode, TraverserReturnType returnType, int pageSize,
                                          int leaseTimeInSeconds, String body )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.createPagedTraverser( startNode, returnType, pageSize,
                    leaseTimeInSeconds, body );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response pagedTraverse( String traverserId,
                                   TraverserReturnType returnType )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.pagedTraverse( traverserId, returnType );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response removePagedTraverser( String traverserId )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.removePagedTraverser( traverserId );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response deleteFromRelationshipIndex( String indexName, long id )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response deleteFromRelationshipIndexNoValue( String indexName, String key, long id )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response deleteFromRelationshipIndex( String indexName, String key, String value, long id )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteFromRelationshipIndex( indexName, key, value, id );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response deleteFromNodeIndexNoKeyValue( String indexName, long id )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response deleteFromNodeIndexNoValue( String indexName, String key, long id )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response deleteFromNodeIndex( String indexName, String key, String value, long id )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteFromNodeIndex( indexName, key, value, id );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getIndexedRelationshipsByQuery( String indexName,
                                                    String key, String query, String order )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getIndexedRelationshipsByQuery( indexName, key, query, order );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getIndexedRelationshipsByQuery( String indexName,
                                                    String query, String order )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getIndexedRelationshipsByQuery( indexName, query, order );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response stopAutoIndexingProperty( String type, String property )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.stopAutoIndexingProperty( type, property );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response startAutoIndexingProperty( String type, String property )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.startAutoIndexingProperty( type, property );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getAutoIndexedProperties( String type )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getAutoIndexedProperties( type );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response setAutoIndexerEnabled( String type, String enable )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.setAutoIndexerEnabled( type, enable );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response isAutoIndexerEnabled( String type )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.isAutoIndexerEnabled( type );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getIndexedRelationships( String indexName, String key,
                                             String value )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getIndexedRelationships( indexName, key, value );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getIndexedNodesByQuery( String indexName, String key,
                                            String query, String order )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getIndexedNodesByQuery( indexName, key, query, order );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getAutoIndexedNodes( String type, String key, String value )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response getRelationshipFromIndexUri( String indexName, String key, String value, long id )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getRelationshipFromIndexUri( indexName, key, value, id );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getNodeFromIndexUri( String indexName, String key, String value, long id )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getNodeFromIndexUri( indexName, key, value, id );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response deleteRelationshipIndex( String indexName )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteRelationshipIndex( indexName );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getAutoIndexedNodesByQuery( String type, String query )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Response getIndexedNodesByQuery( String indexName, String
            query, String order )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getIndexedNodesByQuery( indexName, query, order );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response jsonCreateRelationshipIndex( String json )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.jsonCreateRelationshipIndex( json );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response jsonCreateNodeIndex( String json )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.jsonCreateNodeIndex( json );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getNodeIndexRoot()
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getNodeIndexRoot();
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response deleteRelationshipProperty( long relationshipId, String key )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteRelationshipProperty( relationshipId, key );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response setAllRelationshipProperties( long relationshipId, String body )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.setAllRelationshipProperties( relationshipId, body );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getRelationshipProperty( long relationshipId,
                                             String key )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getRelationshipProperty( relationshipId, key );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getAllRelationshipProperties( long relationshipId )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getAllRelationshipProperties( relationshipId );
        }
        finally
        {
            transaction.finish();
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
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteRelationship( relationshipId );
            transaction.success();
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getRelationship( long relationshipId )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getRelationship( relationshipId );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getAllLabels()
    {
        throw new UnsupportedOperationException( "TODO" );
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
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteNodeProperty( nodeId, key );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getNodeProperty( long
            nodeId, String key )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getNodeProperty( nodeId, key );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response setNodeProperty( long
            nodeId, String key, String body )
    {
        try (Transaction transaction = graph.beginTx())
        {
            Response response = restfulGraphDatabase.setNodeProperty( nodeId, key, body );
            if (response.getStatus() < 300)
            {
                transaction.success();
            }
            return response;
        }
    }

    @Override
    public Response setAllNodeProperties( long nodeId, String body )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.setAllNodeProperties( nodeId, body );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response deleteNode( long nodeId )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            Response response = restfulGraphDatabase.deleteNode( nodeId );
            if ( response.getStatus() < 300 )
            {
                transaction.success();
            }
            return response;
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getNode( long nodeId )
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getNode( nodeId );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Override
    public Response getRoot()
    {
        Transaction transaction = graph.beginTx();
        try
        {
            return restfulGraphDatabase.getRoot();
        }
        finally
        {
            transaction.finish();
        }
    }
}
