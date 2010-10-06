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

package org.neo4j.shell.kernel;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.impl.traversal.OldTraverserWrapper;

public class ReadOnlyGraphDatabaseProxy implements GraphDatabaseService
{
    private final GraphDatabaseService actual;

    ReadOnlyGraphDatabaseProxy( GraphDatabaseService graphDb )
    {
        this.actual = graphDb;
    }

    public GraphDatabaseService getActualGraphDb()
    {
        return actual;
    }

    public Node readOnly( Node actual )
    {
        return new ReadOnlyNodeProxy( actual );
    }

    public Relationship readOnly( Relationship actual )
    {
        return new ReadOnlyRelationshipProxy( actual );
    }

    private static <T> T readOnly()
    {
        throw new UnsupportedOperationException( "Read only Graph Database!" );
    }

    public Transaction beginTx()
    {
        // return readOnly();
        return new Transaction()
        {
            public void success()
            {
            }

            public void failure()
            {
            }

            public void finish()
            {
            }
        };
    }

    public Node createNode()
    {
        return readOnly();
    }

    public boolean enableRemoteShell()
    {
        throw new UnsupportedOperationException( "Cannot enable Remote Shell from Remote Shell" );
    }

    public boolean enableRemoteShell( Map<String, Serializable> initialProperties )
    {
        return enableRemoteShell();
    }

    public Iterable<Node> getAllNodes()
    {
        return nodes( actual.getAllNodes() );
    }

    public Node getNodeById( long id )
    {
        return new ReadOnlyNodeProxy( actual.getNodeById( id ) );
    }

    public Node getReferenceNode()
    {
        return new ReadOnlyNodeProxy( actual.getReferenceNode() );
    }

    public Relationship getRelationshipById( long id )
    {
        return new ReadOnlyRelationshipProxy( actual.getRelationshipById( id ) );
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return actual.getRelationshipTypes();
    }

    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return readOnly();
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return readOnly();
    }

    public void shutdown()
    {
        actual.shutdown();
    }

    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return readOnly();
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return readOnly();
    }

    private class ReadOnlyNodeProxy implements Node
    {
        private final Node actual;

        ReadOnlyNodeProxy( Node actual )
        {
            this.actual = actual;
        }

        @Override
        public int hashCode()
        {
            return actual.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return ( obj instanceof Node ) && ( (Node) obj ).getId() == getId();
        }

        @Override
        public String toString()
        {
            return actual.toString();
        }

        public long getId()
        {
            return actual.getId();
        }

        public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
        {
            return readOnly();
        }

        public void delete()
        {
            readOnly();
        }

        public Iterable<Relationship> getRelationships()
        {
            return relationships( actual.getRelationships() );
        }

        public Iterable<Relationship> getRelationships( RelationshipType... types )
        {
            return relationships( actual.getRelationships( types ) );
        }

        public Iterable<Relationship> getRelationships( Direction dir )
        {
            return relationships( actual.getRelationships( dir ) );
        }

        public Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )
        {
            return relationships( actual.getRelationships( type, dir ) );
        }

        public Relationship getSingleRelationship( RelationshipType type, Direction dir )
        {
            return new ReadOnlyRelationshipProxy( actual.getSingleRelationship( type, dir ) );
        }

        public boolean hasRelationship()
        {
            return actual.hasRelationship();
        }

        public boolean hasRelationship( RelationshipType... types )
        {
            return actual.hasRelationship( types );
        }

        public boolean hasRelationship( Direction dir )
        {
            return actual.hasRelationship( dir );
        }

        public boolean hasRelationship( RelationshipType type, Direction dir )
        {
            return actual.hasRelationship( type, dir );
        }

        public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
                ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType,
                Direction direction )
        {
            return OldTraverserWrapper.traverse( this, traversalOrder, stopEvaluator,
                    returnableEvaluator, relationshipType, direction );
        }

        public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
                ReturnableEvaluator returnableEvaluator, RelationshipType firstRelationshipType,
                Direction firstDirection, RelationshipType secondRelationshipType,
                Direction secondDirection )
        {
            return OldTraverserWrapper.traverse( this, traversalOrder, stopEvaluator,
                    returnableEvaluator, firstRelationshipType, firstDirection,
                    secondRelationshipType, secondDirection );
        }

        public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
                ReturnableEvaluator returnableEvaluator, Object... relationshipTypesAndDirections )
        {
            return OldTraverserWrapper.traverse( this, traversalOrder, stopEvaluator,
                    returnableEvaluator, relationshipTypesAndDirections );
        }

        public GraphDatabaseService getGraphDatabase()
        {
            return ReadOnlyGraphDatabaseProxy.this;
        }

        public Object getProperty( String key )
        {
            return actual.getProperty( key );
        }

        public Object getProperty( String key, Object defaultValue )
        {
            return actual.getProperty( key, defaultValue );
        }

        public Iterable<String> getPropertyKeys()
        {
            return actual.getPropertyKeys();
        }

        public Iterable<Object> getPropertyValues()
        {
            return actual.getPropertyValues();
        }

        public boolean hasProperty( String key )
        {
            return actual.hasProperty( key );
        }

        public Object removeProperty( String key )
        {
            return readOnly();
        }

        public void setProperty( String key, Object value )
        {
            readOnly();
        }
    }

    private class ReadOnlyRelationshipProxy implements Relationship
    {
        private final Relationship actual;

        ReadOnlyRelationshipProxy( Relationship actual )
        {
            this.actual = actual;
        }

        @Override
        public int hashCode()
        {
            return actual.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return ( obj instanceof Relationship ) && ( (Relationship) obj ).getId() == getId();
        }

        @Override
        public String toString()
        {
            return actual.toString();
        }

        public long getId()
        {
            return actual.getId();
        }

        public void delete()
        {
            readOnly();
        }

        public Node getEndNode()
        {
            return new ReadOnlyNodeProxy( actual.getEndNode() );
        }

        public Node[] getNodes()
        {
            return new Node[] { getStartNode(), getEndNode() };
        }

        public Node getOtherNode( Node node )
        {
            return new ReadOnlyNodeProxy( actual.getOtherNode( node ) );
        }

        public Node getStartNode()
        {
            return new ReadOnlyNodeProxy( actual.getStartNode() );
        }

        public RelationshipType getType()
        {
            return actual.getType();
        }

        public boolean isType( RelationshipType type )
        {
            return actual.isType( type );
        }

        public GraphDatabaseService getGraphDatabase()
        {
            return ReadOnlyGraphDatabaseProxy.this;
        }

        public Object getProperty( String key )
        {
            return actual.getProperty( key );
        }

        public Object getProperty( String key, Object defaultValue )
        {
            return actual.getProperty( key, defaultValue );
        }

        public Iterable<String> getPropertyKeys()
        {
            return actual.getPropertyKeys();
        }

        public Iterable<Object> getPropertyValues()
        {
            return actual.getPropertyValues();
        }

        public boolean hasProperty( String key )
        {
            return actual.hasProperty( key );
        }

        public Object removeProperty( String key )
        {
            return readOnly();
        }

        public void setProperty( String key, Object value )
        {
            readOnly();
        }
    }

    public Iterable<Node> nodes( Iterable<Node> nodes )
    {
        return new IterableWrapper<Node, Node>( nodes )
        {
            @Override
            protected Node underlyingObjectToObject( Node node )
            {
                return new ReadOnlyNodeProxy( node );
            }
        };
    }

    public Iterable<Relationship> relationships( Iterable<Relationship> relationships )
    {
        return new IterableWrapper<Relationship, Relationship>( relationships )
        {
            @Override
            protected Relationship underlyingObjectToObject( Relationship relationship )
            {
                return new ReadOnlyRelationshipProxy( relationship );
            }
        };
    }
}
