/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;

public class GraphDbHelper
{
    private final Database database;

    public GraphDbHelper( Database database )
    {
        this.database = database;
    }

    public int getNumberOfNodes() throws DatabaseBlockedException
    {
        return numberOfEntitiesFor( Node.class );
    }

    public int getNumberOfRelationships() throws DatabaseBlockedException
    {
        return numberOfEntitiesFor( Relationship.class );
    }

    private int numberOfEntitiesFor( Class<? extends PropertyContainer> type ) throws DatabaseBlockedException
    {
        return (int) ( (EmbeddedGraphDatabase) database.graph ).getConfig()
                .getGraphDbModule()
                .getNodeManager()
                .getNumberOfIdsInUse( type );
    }

    public Map<String, Object> getNodeProperties( long nodeId ) throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Node node = database.graph.getNodeById( nodeId );
            Map<String, Object> allProperties = new HashMap<String, Object>();
            for ( String propertyKey : node.getPropertyKeys() )
            {
                allProperties.put( propertyKey, node.getProperty( propertyKey ) );
            }
            tx.success();
            return allProperties;
        }
        finally
        {
            tx.finish();
        }
    }

    public void setNodeProperties( long nodeId, Map<String, Object> properties ) throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Node node = database.graph.getNodeById( nodeId );
            for ( Map.Entry<String, Object> propertyEntry : properties.entrySet() )
            {
                node.setProperty( propertyEntry.getKey(), propertyEntry.getValue() );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public long createNode() throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Node node = database.graph.createNode();
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }

    public long createNode( Map<String, Object> properties ) throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Node node = database.graph.createNode();
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                node.setProperty( entry.getKey(), entry.getValue() );
            }
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }

    public void deleteNode( long id ) throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Node node = database.graph.getNodeById( id );
            node.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public long createRelationship( String type, long startNodeId, long endNodeId ) throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Node startNode = database.graph.getNodeById( startNodeId );
            Node endNode = database.graph.getNodeById( endNodeId );
            Relationship relationship = startNode.createRelationshipTo( endNode,
                    DynamicRelationshipType.withName( type ) );
            tx.success();
            return relationship.getId();
        }
        finally
        {
            tx.finish();
        }
    }

    public long createRelationship( String type ) throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Node startNode = database.graph.createNode();
            Node endNode = database.graph.createNode();
            Relationship relationship = startNode.createRelationshipTo( endNode,
                    DynamicRelationshipType.withName( type ) );
            tx.success();
            return relationship.getId();
        }
        finally
        {
            tx.finish();
        }
    }

    public void setRelationshipProperties( long relationshipId, Map<String, Object> properties )
            throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Relationship relationship = database.graph.getRelationshipById( relationshipId );
            for ( Map.Entry<String, Object> propertyEntry : properties.entrySet() )
            {
                relationship.setProperty( propertyEntry.getKey(), propertyEntry.getValue() );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public Map<String, Object> getRelationshipProperties( long relationshipId ) throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Relationship relationship = database.graph.getRelationshipById( relationshipId );
            Map<String, Object> allProperties = new HashMap<String, Object>();
            for ( String propertyKey : relationship.getPropertyKeys() )
            {
                allProperties.put( propertyKey, relationship.getProperty( propertyKey ) );
            }
            tx.success();
            return allProperties;
        }
        finally
        {
            tx.finish();
        }
    }

    public Relationship getRelationship( long relationshipId ) throws DatabaseBlockedException
    {
        Transaction tx = database.graph.beginTx();
        try
        {
            Relationship relationship = database.graph.getRelationshipById( relationshipId );
            tx.success();
            return relationship;
        }
        finally
        {
            tx.finish();
        }
    }

    public void addNodeToIndex( String indexName, String key, Object value, long id ) throws DatabaseBlockedException
    {
        Index<Node> index = database.getNodeIndex( indexName );
        Transaction tx = database.graph.beginTx();
        try
        {
            index.add( database.graph.getNodeById( id ), key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
    
    public void enableNodeAutoIndexingFor(String key) {
    	AutoIndexer<Node> nodeAutoIndexer = database.graph.index().getNodeAutoIndexer();
    	nodeAutoIndexer.startAutoIndexingProperty( key );
    	nodeAutoIndexer.setEnabled( true );
    }
    
    public void enableRelationshipAutoIndexingFor(String key) {
    	AutoIndexer<Relationship> relAutoIndexer = database.graph.index().getRelationshipAutoIndexer();
    	relAutoIndexer.startAutoIndexingProperty( key );
    	relAutoIndexer.setEnabled( true );
    }

    public Collection<Long> queryIndexedNodes( String indexName, String key, Object value )
            throws DatabaseBlockedException
    {
        Index<Node> index = database.getNodeIndex( indexName );
        Transaction tx = database.graph.beginTx();
        try
        {
            Collection<Long> result = new ArrayList<Long>();
            for ( Node node : index.query( key, value ) )
            {
                result.add( node.getId() );
            }
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    public Collection<Long> getIndexedNodes( String indexName, String key, Object value )
            throws DatabaseBlockedException
    {

        Index<Node> index = database.getNodeIndex( indexName );
        Transaction tx = database.graph.beginTx();
        try
        {
            Collection<Long> result = new ArrayList<Long>();
            for ( Node node : index.get( key, value ) )
            {
                result.add( node.getId() );
            }
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    public Collection<Long> getIndexedRelationships( String indexName, String key, Object value )
            throws DatabaseBlockedException
    {

        Index<Relationship> index = database.getRelationshipIndex( indexName );
        Transaction tx = database.graph.beginTx();
        try
        {
            Collection<Long> result = new ArrayList<Long>();
            for ( Relationship relationship : index.get( key, value ) )
            {
                result.add( relationship.getId() );
            }
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    public void addRelationshipToIndex( String indexName, String key, String value, long relationshipId )
    {
        Index<Relationship> index = database.getRelationshipIndex( indexName );
        Transaction tx = database.graph.beginTx();
        try
        {
            index.add( database.graph.getRelationshipById( relationshipId ), key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public String[] getNodeIndexes()
    {
        return database.getIndexManager()
                .nodeIndexNames();
    }

    public Index<Node> getNodeIndex( String indexName )
    {
        return database.getIndexManager()
                .forNodes( indexName );
    }

    public Index<Node> createNodeFullTextIndex( String named )
    {
        return database.getIndexManager()
                .forNodes( named, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
    }

    public Index<Node> createNodeIndex( String named )
    {
        return database.getIndexManager()
                .forNodes( named );
    }

    public String[] getRelationshipIndexes()
    {
        return database.getIndexManager()
                .relationshipIndexNames();
    }

    public long getReferenceNode()
    {
        return database.graph.getReferenceNode()
                .getId();
    }

    public Index<Relationship> getRelationshipIndex( String indexName )
    {
        return database.getIndexManager()
                .forRelationships( indexName );
    }

    public Index<Relationship> createRelationshipFullTextIndex( String named )
    {
        return database.getIndexManager()
                .forRelationships( named, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
    }

    public Index<Relationship> createRelationshipIndex( String named )
    {
        return database.getIndexManager()
                .forRelationships( named );
    }
}
