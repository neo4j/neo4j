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

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.IterableWrapper;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.server.database.DatabaseService;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class GraphDbHelper
{
    private final DatabaseService database;

    public GraphDbHelper( DatabaseService database )
    {
        this.database = database;
    }

    public int getNumberOfNodes()
    {
        Kernel kernel = database.getDatabase().getDependencyResolver().resolveDependency( Kernel.class );
        try ( org.neo4j.internal.kernel.api.Transaction tx = kernel.beginTransaction( implicit, AnonymousContext.read() ) )
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
        Kernel kernel = database.getDatabase().getDependencyResolver().resolveDependency( Kernel.class );
        try ( org.neo4j.internal.kernel.api.Transaction tx = kernel.beginTransaction( implicit, AnonymousContext.read() ) )
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
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            Node node = database.getDatabase().getNodeById( nodeId );
            Map<String, Object> allProperties = node.getAllProperties();
            tx.commit();
            return allProperties;
        }
    }

    public void setNodeProperties( long nodeId, Map<String, Object> properties )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Node node = database.getDatabase().getNodeById( nodeId );
            for ( Map.Entry<String, Object> propertyEntry : properties.entrySet() )
            {
                node.setProperty( propertyEntry.getKey(), propertyEntry.getValue() );
            }
            tx.commit();
        }
    }

    public long createNode( Label... labels )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Node node = database.getDatabase().createNode( labels );
            tx.commit();
            return node.getId();
        }
    }

    public long createNode( Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Node node = database.getDatabase().createNode( labels );
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                node.setProperty( entry.getKey(), entry.getValue() );
            }
            tx.commit();
            return node.getId();
        }
    }

    public void deleteNode( long id )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.write() ) )
        {
            Node node = database.getDatabase().getNodeById( id );
            node.delete();
            tx.commit();
        }
    }

    public long createRelationship( String type, long startNodeId, long endNodeId )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Node startNode = database.getDatabase().getNodeById( startNodeId );
            Node endNode = database.getDatabase().getNodeById( endNodeId );
            Relationship relationship = startNode.createRelationshipTo( endNode, RelationshipType.withName( type ) );
            tx.commit();
            return relationship.getId();
        }
    }

    public long createRelationship( String type )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Node startNode = database.getDatabase().createNode();
            Node endNode = database.getDatabase().createNode();
            Relationship relationship = startNode.createRelationshipTo( endNode,
                    RelationshipType.withName( type ) );
            tx.commit();
            return relationship.getId();
        }
    }

    public void setRelationshipProperties( long relationshipId, Map<String, Object> properties )

    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            Relationship relationship = database.getDatabase().getRelationshipById( relationshipId );
            for ( Map.Entry<String, Object> propertyEntry : properties.entrySet() )
            {
                relationship.setProperty( propertyEntry.getKey(), propertyEntry.getValue() );
            }
            tx.commit();
        }
    }

    public Map<String, Object> getRelationshipProperties( long relationshipId )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            Relationship relationship = database.getDatabase().getRelationshipById( relationshipId );
            Map<String, Object> allProperties = relationship.getAllProperties();
            tx.commit();
            return allProperties;
        }
    }

    public Relationship getRelationship( long relationshipId )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            Relationship relationship = database.getDatabase().getRelationshipById( relationshipId );
            tx.commit();
            return relationship;
        }
    }

    public long getFirstNode()
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.write() ) )
        {
            try
            {
                Node referenceNode = database.getDatabase().getNodeById( 0L );

                tx.commit();
                return referenceNode.getId();
            }
            catch ( NotFoundException e )
            {
                Node newNode = database.getDatabase().createNode();
                tx.commit();
                return newNode.getId();
            }
        }
    }

    public Iterable<String> getNodeLabels( long node )
    {
        return new IterableWrapper<String, Label>( database.getDatabase().getNodeById( node ).getLabels() )
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
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.writeToken() ) )
        {
            database.getDatabase().getNodeById( node ).addLabel( label( labelName ) );
            tx.commit();
        }
    }

    public Iterable<IndexDefinition> getSchemaIndexes( String labelName )
    {
        return database.getDatabase().schema().getIndexes( label( labelName ) );
    }

    public IndexDefinition createSchemaIndex( String labelName, String propertyKey )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AUTH_DISABLED ) )
        {
            IndexDefinition index = database.getDatabase().schema().indexFor( label( labelName ) ).on( propertyKey ).create();
            tx.commit();
            return index;
        }
    }

    public Iterable<ConstraintDefinition> getPropertyUniquenessConstraints( String labelName, final String propertyKey )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AnonymousContext.read() ) )
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

            }, database.getDatabase().schema().getConstraints( label( labelName ) ) );
            tx.commit();
            return definitions;
        }
    }

    public ConstraintDefinition createPropertyUniquenessConstraint( String labelName, List<String> propertyKeys )
    {
        try ( Transaction tx = database.getDatabase().beginTransaction( implicit, AUTH_DISABLED ) )
        {
            ConstraintCreator creator = database.getDatabase().schema().constraintFor( label( labelName ) );
            for ( String propertyKey : propertyKeys )
            {
                creator = creator.assertPropertyIsUnique( propertyKey );
            }
            ConstraintDefinition result = creator.create();
            tx.commit();
            return result;
        }
    }

    public long getLabelCount( long nodeId )
    {
        try ( Transaction transaction = database.getDatabase().beginTransaction( implicit, AnonymousContext.read() ) )
        {
            return count( database.getDatabase().getNodeById( nodeId ).getLabels());
        }
    }
}
