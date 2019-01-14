/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.rest.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
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
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.server.database.Database;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class GraphDbHelper
{
    private final Database database;

    public GraphDbHelper( Database database )
    {
        this.database = database;
    }

    public int getNumberOfNodes()
    {
        Kernel kernel = database.getGraph().getDependencyResolver().resolveDependency( Kernel.class );
        try ( Session session = kernel.beginSession( AnonymousContext.read() );
              org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction( org.neo4j.internal.kernel.api.Transaction.Type.implicit ) )
        {
            return Math.toIntExact( tx.dataRead().nodesGetCount() );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    public int getNumberOfRelationships()
    {
        Kernel kernel = database.getGraph().getDependencyResolver().resolveDependency( Kernel.class );
        try ( Session session = kernel.beginSession( AnonymousContext.read() );
              org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction( org.neo4j.internal.kernel.api.Transaction.Type.implicit ) )
        {
            return Math.toIntExact( tx.dataRead().relationshipsGetCount() );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    public Map<String, Object> getNodeProperties( long nodeId )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            Node node = database.getGraph().getNodeById( nodeId );
            Map<String, Object> allProperties = node.getAllProperties();
            tx.success();
            return allProperties;
        }
    }

    public void setNodeProperties( long nodeId, Map<String, Object> properties )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.writeToken() ) )
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
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Node node = database.getGraph().createNode( labels );
            tx.success();
            return node.getId();
        }
    }

    public long createNode( Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.writeToken() ) )
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
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.write() ) )
        {
            Node node = database.getGraph().getNodeById( id );
            node.delete();
            tx.success();
        }
    }

    public long createRelationship( String type, long startNodeId, long endNodeId )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Node startNode = database.getGraph().getNodeById( startNodeId );
            Node endNode = database.getGraph().getNodeById( endNodeId );
            Relationship relationship = startNode.createRelationshipTo( endNode, RelationshipType.withName( type ) );
            tx.success();
            return relationship.getId();
        }
    }

    public long createRelationship( String type )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Node startNode = database.getGraph().createNode();
            Node endNode = database.getGraph().createNode();
            Relationship relationship = startNode.createRelationshipTo( endNode,
                    RelationshipType.withName( type ) );
            tx.success();
            return relationship.getId();
        }
    }

    public void setRelationshipProperties( long relationshipId, Map<String, Object> properties )

    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.writeToken() ) )
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
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            Relationship relationship = database.getGraph().getRelationshipById( relationshipId );
            Map<String, Object> allProperties = relationship.getAllProperties();
            tx.success();
            return allProperties;
        }
    }

    public Relationship getRelationship( long relationshipId )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            Relationship relationship = database.getGraph().getRelationshipById( relationshipId );
            tx.success();
            return relationship;
        }
    }

    public void addNodeToIndex( String indexName, String key, Object value, long id )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AUTH_DISABLED ) )
        {
            database.getGraph().index().forNodes( indexName ).add( database.getGraph().getNodeById( id ), key, value );
            tx.success();
        }
    }

    public Collection<Long> queryIndexedNodes( String indexName, String key, Object value )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.write() ) )
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
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.write() ) )
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
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.write() ) )
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
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AUTH_DISABLED ) )
        {
            Index<Relationship> index = database.getGraph().index().forRelationships( indexName );
            index.add( database.getGraph().getRelationshipById( relationshipId ), key, value );
            tx.success();
        }

    }

    public String[] getNodeIndexes()
    {
        try ( Transaction transaction = database.getGraph().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            return database.getGraph().index().nodeIndexNames();
        }
    }

    public Index<Node> createNodeFullTextIndex( String named )
    {
        try ( Transaction transaction = database.getGraph().beginTransaction( implicit, AUTH_DISABLED ) )
        {
            Index<Node> index = database.getGraph().index()
                    .forNodes( named, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
            transaction.success();
            return index;
        }
    }

    public Index<Node> createNodeIndex( String named )
    {
        try ( Transaction transaction = database.getGraph().beginTransaction( implicit, AUTH_DISABLED ) )
        {
            Index<Node> nodeIndex = database.getGraph().index()
                    .forNodes( named );
            transaction.success();
            return nodeIndex;
        }
    }

    public String[] getRelationshipIndexes()
    {
        try ( Transaction transaction = database.getGraph().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            return database.getGraph().index().relationshipIndexNames();
        }
    }

    public long getFirstNode()
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.write() ) )
        {
            try
            {
                Node referenceNode = database.getGraph().getNodeById( 0L );

                tx.success();
                return referenceNode.getId();
            }
            catch ( NotFoundException e )
            {
                Node newNode = database.getGraph().createNode();
                tx.success();
                return newNode.getId();
            }
        }
    }

    public Index<Relationship> createRelationshipIndex( String named )
    {
        try ( Transaction transaction = database.getGraph().beginTransaction( implicit, AUTH_DISABLED ) )
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
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.writeToken() ) )
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
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AUTH_DISABLED ) )
        {
            IndexDefinition index = database.getGraph().schema().indexFor( label( labelName ) ).on( propertyKey ).create();
            tx.success();
            return index;
        }
    }

    public Iterable<ConstraintDefinition> getPropertyUniquenessConstraints( String labelName, final String propertyKey )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            Iterable<ConstraintDefinition> definitions = Iterables.filter( item ->
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

            }, database.getGraph().schema().getConstraints( label( labelName ) ) );
            tx.success();
            return definitions;
        }
    }

    public ConstraintDefinition createPropertyUniquenessConstraint( String labelName, List<String> propertyKeys )
    {
        try ( Transaction tx = database.getGraph().beginTransaction( implicit, AUTH_DISABLED ) )
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

    public long getLabelCount( long nodeId )
    {
        try ( Transaction transaction = database.getGraph().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            return count( database.getGraph().getNodeById( nodeId ).getLabels());
        }
    }
}
