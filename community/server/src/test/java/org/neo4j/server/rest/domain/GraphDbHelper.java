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
package org.neo4j.server.rest.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.DynamicLabel;
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
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.coreapi.schema.InternalSchemaActions;
import org.neo4j.kernel.impl.coreapi.schema.NodePropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.RelationshipPropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.server.database.Database;

import static org.mockito.Mockito.mock;
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
        return (int) database.getGraph().getDependencyResolver().resolveDependency( NeoStoresSupplier.class ).get()
                .getNodeStore().getNumberOfIdsInUse();
    }

    public int getNumberOfRelationships()
    {
        return (int) database.getGraph().getDependencyResolver().resolveDependency( NeoStoresSupplier.class ).get()
                .getRelationshipStore().getNumberOfIdsInUse();
    }


    public Map<String, Object> getNodeProperties( long nodeId )
    {
        try (Transaction tx = database.getGraph().beginTx())
        {
            Node node = database.getGraph().getNodeById( nodeId );
            Map<String, Object> allProperties = node.getAllProperties();
            tx.success();
            return allProperties;
        }
    }

    public void setNodeProperties( long nodeId, Map<String, Object> properties )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().getNodeById( nodeId );
            for ( Map.Entry<String, Object> propertyEntry : properties.entrySet() )
            {
                node.setProperty( propertyEntry.getKey(), propertyEntry.getValue() );
            }
            tx.success();
        }
    }

    public long createNode( Label... labels )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().createNode( labels );
            tx.success();
            return node.getId();
        }
    }

    public long createNode( Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node node = database.getGraph().createNode( labels );
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                node.setProperty( entry.getKey(), entry.getValue() );
            }
            tx.success();
            return node.getId();
        }
    }

    public void deleteNode( long id )
    {
        try (Transaction tx = database.getGraph().beginTx())
        {
            Node node = database.getGraph().getNodeById( id );
            node.delete();
            tx.success();
        }
    }

    public long createRelationship( String type, long startNodeId, long endNodeId )
    {
        try (Transaction tx = database.getGraph().beginTx())
        {
            Node startNode = database.getGraph().getNodeById( startNodeId );
            Node endNode = database.getGraph().getNodeById( endNodeId );
            Relationship relationship = startNode.createRelationshipTo( endNode,
                    DynamicRelationshipType.withName( type ) );
            tx.success();
            return relationship.getId();
        }
    }

    public long createRelationship( String type )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Node startNode = database.getGraph().createNode();
            Node endNode = database.getGraph().createNode();
            Relationship relationship = startNode.createRelationshipTo( endNode,
                    DynamicRelationshipType.withName( type ) );
            tx.success();
            return relationship.getId();
        }
    }

    public void setRelationshipProperties( long relationshipId, Map<String, Object> properties )

    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Relationship relationship = database.getGraph().getRelationshipById( relationshipId );
            for ( Map.Entry<String, Object> propertyEntry : properties.entrySet() )
            {
                relationship.setProperty( propertyEntry.getKey(), propertyEntry.getValue() );
            }
            tx.success();
        }
    }

    public Map<String, Object> getRelationshipProperties( long relationshipId )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Relationship relationship = database.getGraph().getRelationshipById( relationshipId );
            Map<String, Object> allProperties = relationship.getAllProperties();
            tx.success();
            return allProperties;
        }
    }

    public Relationship getRelationship( long relationshipId )
    {
        try (Transaction tx = database.getGraph().beginTx())
        {
            Relationship relationship = database.getGraph().getRelationshipById( relationshipId );
            tx.success();
            return relationship;
        }
    }

    public void addNodeToIndex( String indexName, String key, Object value, long id )
    {
        try (Transaction tx = database.getGraph().beginTx())
        {
            database.getGraph().index().forNodes( indexName ).add( database.getGraph().getNodeById( id ), key, value );
            tx.success();
        }
    }

    public Collection<Long> queryIndexedNodes( String indexName, String key, Object value )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Collection<Long> result = new ArrayList<>();
            for ( Node node : database.getGraph().index().forNodes( indexName ).query( key, value ) )
            {
                result.add( node.getId() );
            }
            tx.success();
            return result;
        }
    }

    public Collection<Long> getIndexedNodes( String indexName, String key, Object value )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Collection<Long> result = new ArrayList<>();
            for ( Node node : database.getGraph().index().forNodes( indexName ).get( key, value ) )
            {
                result.add( node.getId() );
            }
            tx.success();
            return result;
        }
    }

    public Collection<Long> getIndexedRelationships( String indexName, String key, Object value )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Collection<Long> result = new ArrayList<>();
            for ( Relationship relationship : database.getGraph().index().forRelationships( indexName ).get( key, value ) )
            {
                result.add( relationship.getId() );
            }
            tx.success();
            return result;
        }
    }

    public void addRelationshipToIndex( String indexName, String key, String value, long relationshipId )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Index<Relationship> index = database.getGraph().index().forRelationships( indexName );
            index.add( database.getGraph().getRelationshipById( relationshipId ), key, value );
            tx.success();
        }

    }

    public String[] getNodeIndexes()
    {
        try (Transaction transaction = database.getGraph().beginTx())
        {
            return database.getGraph().index().nodeIndexNames();
        }
    }

    public Index<Node> createNodeFullTextIndex( String named )
    {
        try ( Transaction transaction = database.getGraph().beginTx() )
        {
            Index<Node> index = database.getGraph().index().forNodes( named, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
            transaction.success();
            return index;
        }
    }

    public Index<Node> createNodeIndex( String named )
    {
        try ( Transaction transaction = database.getGraph().beginTx() )
        {
            Index<Node> nodeIndex = database.getGraph().index()
                    .forNodes( named );
            transaction.success();
            return nodeIndex;
        }
    }

    public String[] getRelationshipIndexes()
    {
        try (Transaction transaction = database.getGraph().beginTx())
        {
            return database.getGraph().index()
                    .relationshipIndexNames();
        }
    }

    public long getFirstNode()
    {
        try (Transaction tx = database.getGraph().beginTx())
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
    }

    public Index<Relationship> createRelationshipIndex( String named )
    {
        try (Transaction transaction = database.getGraph().beginTx())
        {
            RelationshipIndex relationshipIndex = database.getGraph().index()
                    .forRelationships( named );
            transaction.success();
            return relationshipIndex;
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
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            database.getGraph().getNodeById( node ).addLabel( label( labelName ) );
            tx.success();
        }
    }

    public Iterable<IndexDefinition> getSchemaIndexes( String labelName )
    {
        return database.getGraph().schema().getIndexes( label( labelName ) );
    }

    public IndexDefinition createSchemaIndex( String labelName, String propertyKey )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            IndexDefinition index = database.getGraph().schema().indexFor( label( labelName ) ).on( propertyKey ).create();
            tx.success();
            return index;
        }
    }

    public Iterable<ConstraintDefinition> getPropertyUniquenessConstraints( String labelName, final String propertyKey )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Iterable<ConstraintDefinition> definitions = Iterables.filter( new Predicate<ConstraintDefinition>()
            {

                @Override
                public boolean test( ConstraintDefinition item )
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
    }

    public Iterable<ConstraintDefinition> getNodePropertyExistenceConstraints( String labelName,
            final String propertyKey )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            Iterable<ConstraintDefinition> definitions = filterByConstraintTypeAndPropertyKey(
                    database.getGraph().schema().getConstraints( label( labelName ) ),
                    ConstraintType.NODE_PROPERTY_EXISTENCE, propertyKey );
            tx.success();
            return definitions;
        }
    }

    public Iterable<ConstraintDefinition> getRelationshipPropertyExistenceConstraints( String typeName,
            final String propertyKey )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            DynamicRelationshipType type = DynamicRelationshipType.withName( typeName );
            Iterable<ConstraintDefinition> definitions = filterByConstraintTypeAndPropertyKey(
                    database.getGraph().schema().getConstraints( type ),
                    ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE, propertyKey );
            tx.success();
            return definitions;
        }
    }

    public ConstraintDefinition createPropertyUniquenessConstraint( String labelName, List<String> propertyKeys )
    {
        try ( Transaction tx = database.getGraph().beginTx() )
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
    }

    public ConstraintDefinition createNodePropertyExistenceConstraint( String labelName, String propertyKey )
    {
        String query = String.format( "CREATE CONSTRAINT ON (n:%s) ASSERT exists(n.%s)", labelName, propertyKey );
        database.getGraph().execute( query );
        awaitIndexes();
        return new NodePropertyExistenceConstraintDefinition( mock( InternalSchemaActions.class ),
                DynamicLabel.label( labelName ), propertyKey );
    }

    public ConstraintDefinition createRelationshipPropertyExistenceConstraint( String typeName, String propertyKey )
    {
        String query = String.format( "CREATE CONSTRAINT ON ()-[r:%s]-() ASSERT exists(r.%s)", typeName, propertyKey );
        database.getGraph().execute( query );
        awaitIndexes();
        return new RelationshipPropertyExistenceConstraintDefinition( mock( InternalSchemaActions.class ),
                DynamicRelationshipType.withName( typeName ), propertyKey );
    }

    public long getLabelCount( long nodeId )
    {
        try (Transaction transaction = database.getGraph().beginTx())
        {
            return count( database.getGraph().getNodeById( nodeId ).getLabels());
        }
    }

    private static Iterable<ConstraintDefinition> filterByConstraintTypeAndPropertyKey(
            Iterable<ConstraintDefinition> definitions, final ConstraintType type, final String propertyKey )
    {
        return Iterables.filter( new Predicate<ConstraintDefinition>()
        {
            @Override
            public boolean test( ConstraintDefinition definition )
            {
                if ( definition.isConstraintType( type ) )
                {
                    Iterable<String> keys = definition.getPropertyKeys();
                    return single( keys ).equals( propertyKey );
                }
                return false;
            }
        }, definitions );
    }

    private void awaitIndexes()
    {
        try ( Transaction tx = database.getGraph().beginTx() )
        {
            database.getGraph().schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }
}
