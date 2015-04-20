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
package org.neo4j.server.rest.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.server.database.Database;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;

public class GraphDbHelper
{
    private final Database database;

    public GraphDbHelper( Database database )
    {
        this.database = database;
    }

    public int getNumberOfNodes()
    {
        return (int) database.getGraph().getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate()
                .getNodeStore().getNumberOfIdsInUse();
    }

    public int getNumberOfRelationships()
    {
        return (int) database.getGraph().getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate()
                .getRelationshipStore().getNumberOfIdsInUse();
    }


    public Map<String, Object> getNodeProperties( long nodeId )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Node node = database.getGraph().getNodeById( nodeId );
            Map<String, Object> allProperties = new HashMap<>();
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

    public void setNodeProperties( long nodeId, Map<String, Object> properties )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Node node = database.getGraph().getNodeById( nodeId );
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

    public long createNode( Label... labels )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Node node = database.getGraph().createNode( labels );
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }

    public long createNode( Map<String, Object> properties, Label... labels )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Node node = database.getGraph().createNode( labels );
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

    public void deleteNode( long id )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Node node = database.getGraph().getNodeById( id );
            node.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public long createRelationship( String type, long startNodeId, long endNodeId )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Node startNode = database.getGraph().getNodeById( startNodeId );
            Node endNode = database.getGraph().getNodeById( endNodeId );
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

    public long createRelationship( String type )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Node startNode = database.getGraph().createNode();
            Node endNode = database.getGraph().createNode();
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

    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Relationship relationship = database.getGraph().getRelationshipById( relationshipId );
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

    public Map<String, Object> getRelationshipProperties( long relationshipId )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Relationship relationship = database.getGraph().getRelationshipById( relationshipId );
            Map<String, Object> allProperties = new HashMap<>();
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

    public Relationship getRelationship( long relationshipId )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Relationship relationship = database.getGraph().getRelationshipById( relationshipId );
            tx.success();
            return relationship;
        }
        finally
        {
            tx.finish();
        }
    }

    public void addNodeToIndex( String indexName, String key, Object value, long id )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            database.getGraph().index().forNodes( indexName ).add( database.getGraph().getNodeById( id ), key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public Collection<Long> queryIndexedNodes( String indexName, String key, Object value )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Collection<Long> result = new ArrayList<>();
            for ( Node node : database.getGraph().index().forNodes( indexName ).query( key, value ) )
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
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Collection<Long> result = new ArrayList<>();
            for ( Node node : database.getGraph().index().forNodes( indexName ).get( key, value ) )
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
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Collection<Long> result = new ArrayList<>();
            for ( Relationship relationship : database.getGraph().index().forRelationships( indexName ).get( key, value ) )
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
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Index<Relationship> index = database.getGraph().index().forRelationships( indexName );
            index.add( database.getGraph().getRelationshipById( relationshipId ), key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public String[] getNodeIndexes()
    {
        Transaction transaction = database.getGraph().beginTx();
        try
        {
            return database.getGraph().index().nodeIndexNames();
        }
        finally
        {
            transaction.finish();
        }
    }

    public Index<Node> createNodeFullTextIndex( String named )
    {
        Transaction transaction = database.getGraph().beginTx();
        try
        {
            Index<Node> index = database.getGraph().index().forNodes( named, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
            transaction.success();
            return index;
        }
        finally
        {
            transaction.finish();
        }
    }

    public Index<Node> createNodeIndex( String named )
    {
        Transaction transaction = database.getGraph().beginTx();
        try
        {
            Index<Node> nodeIndex = database.getGraph().index()
                    .forNodes( named );
            transaction.success();
            return nodeIndex;
        }
        finally
        {
            transaction.finish();
        }
    }

    public String[] getRelationshipIndexes()
    {
        Transaction transaction = database.getGraph().beginTx();
        try
        {
            return database.getGraph().index()
                    .relationshipIndexNames();
        }
        finally
        {
            transaction.finish();
        }
    }

    public long getFirstNode()
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            try
            {
                Node referenceNode = database.getGraph().getNodeById(0l);

                tx.success();
                return referenceNode.getId();
            }
            catch(NotFoundException e)
            {
                Node newNode = database.getGraph().createNode();
                tx.success();
                return newNode.getId();
            }
        }
        finally
        {
            tx.finish();
        }
    }

    public Index<Relationship> createRelationshipIndex( String named )
    {
        Transaction transaction = database.getGraph().beginTx();
        try
        {
            RelationshipIndex relationshipIndex = database.getGraph().index()
                    .forRelationships( named );
            transaction.success();
            return relationshipIndex;
        }
        finally
        {
            transaction.finish();
        }
    }

    public Iterable<String> getNodeLabels( long node )
    {
        return new IterableWrapper<String, Label>( database.getGraph().getNodeById( node ).getLabels() )
        {
            @Override
            protected String underlyingObjectToObject( Label object )
            {
                return object.name();
            }
        };
    }

    public void addLabelToNode( long node, String labelName )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            database.getGraph().getNodeById( node ).addLabel( label( labelName ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public Iterable<IndexDefinition> getSchemaIndexes( String labelName )
    {
        return database.getGraph().schema().getIndexes( label( labelName ) );
    }

    public IndexDefinition createSchemaIndex( String labelName, String propertyKey )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            IndexDefinition index = database.getGraph().schema().indexFor( label( labelName ) ).on( propertyKey ).create();
            tx.success();
            return index;
        }
        finally
        {
            tx.finish();
        }
    }

    public Iterable<ConstraintDefinition> getPropertyUniquenessConstraints( String labelName, final String propertyKey )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            Iterable<ConstraintDefinition> definitions = Iterables.filter( new Predicate<ConstraintDefinition>()
            {

                @Override
                public boolean accept( ConstraintDefinition item )
                {
                    if ( item.isConstraintType( ConstraintType.UNIQUENESS ) )
                    {
                        Iterable<String> keys = item.getPropertyKeys();
                        return single( keys ).equals( propertyKey );
                    }
                    else
                    {
                        return false;
                    }

                }
            }, database.getGraph().schema().getConstraints( label( labelName ) ) );
            tx.success();
            return definitions;
        }
        finally
        {
            tx.finish();
        }
    }

    public ConstraintDefinition createPropertyUniquenessConstraint( String labelName, List<String> propertyKeys )
    {
        Transaction tx = database.getGraph().beginTx();
        try
        {
            ConstraintCreator creator = database.getGraph().schema().constraintFor( label( labelName ) );
            for ( String propertyKey : propertyKeys )
            {
                creator = creator.assertPropertyIsUnique( propertyKey );
            }
            ConstraintDefinition result = creator.create();
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    public long getLabelCount( long nodeId )
    {
        Transaction transaction = database.getGraph().beginTx();

        try
        {
            return count( database.getGraph().getNodeById( nodeId ).getLabels());
        }
        finally
        {
            transaction.finish();
        }
    }
}
