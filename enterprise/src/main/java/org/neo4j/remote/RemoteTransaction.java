/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.remote;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.remote.RemoteGraphDbEngine.BatchIterable;
import org.neo4j.remote.RemoteGraphDbEngine.CloseableIteratorWithSize;
import org.neo4j.index.IndexHits;

class RemoteTransaction implements Transaction
{
    private enum State
    {
        /**
         * The state of this transaction is not defined yet. This means failure.
         */
        UNDEFINED( false )
        {
            @Override
            void update( RemoteTransaction tx )
            {
            }
        },
        /**
         * The state of this transaction is success.
         */
        SUCCESS( false )
        {
            @Override
            void update( RemoteTransaction tx )
            {
                if ( !tx.state.terminal )
                {
                    tx.state = this;
                }
                else
                {
                    tx.state.update( tx );
                }
            }

            @Override
            void finish( RemoteTransaction tx )
            {
                tx.state = COMPLETED;
                tx.commit();
            }
        },
        /**
         * The state of this transaction is failure and can never be success.
         */
        FAILURE( true )
        {
            @Override
            void update( RemoteTransaction tx )
            {
                tx.state = this;
            }
        },
        /**
         * The state of this transaction is completed.
         */
        COMPLETED( true )
        {
            @Override
            void update( RemoteTransaction tx )
            {
                this.finish( tx );
            }

            @Override
            void finish( RemoteTransaction tx )
            {
                throw new IllegalStateException( "The transaction " + tx
                    + " is completed." );
            }
        };
        private final boolean terminal;

        private State( boolean terminal )
        {
            this.terminal = terminal;
        }

        abstract void update( RemoteTransaction tx );

        void finish( RemoteTransaction tx )
        {
            tx.state = COMPLETED;
            tx.rollback();
        }
    }

    private State state = State.UNDEFINED;

    public void failure()
    {
        State.FAILURE.update( this );
    }

    public void success()
    {
        State.SUCCESS.update( this );
    }

    public void finish()
    {
        state.finish( this );
        engine.endTx( null );
    }

    // Internal state

    private final RemoteGraphDbEngine engine;
    final int id;
    private final Map<Long, RemoteNode> nodeCache = null;
    private final Map<Long, RemoteRelationship> relationshipCache = null;

    @Override
    public String toString()
    {
        return "RemoteTransaction[" + engine + ", id=" + id + "]";
    }

    RemoteTransaction( RemoteGraphDbEngine txService, int id )
    {
        this.engine = txService;
        this.id = id;
    }

    RemoteTransaction createPlaceboTransaction()
    {
        final RemoteTransaction previous = this;
        return new RemoteTransaction( engine, id )
        {
            @Override
            public void failure()
            {
                State.FAILURE.update( previous );
            }

            @Override
            public void success()
            {
                State.SUCCESS.update( previous );
            }

            @Override
            public void finish()
            {
            }

            @Override
            RemoteTransaction createPlaceboTransaction()
            {
                return previous.createPlaceboTransaction();
            }

            @Override
            public String toString()
            {
                return "Placebo" + super.toString();
            }
        };
    }

    // Implementation internals

    // Transaction management

    private void commit()
    {
        engine.commit( id );
    }

    private void rollback()
    {
        engine.rollback( id );
    }

    // Node management

    private RemoteNode newNode( long nodeId )
    {
        RemoteNode node = new RemoteNode( engine, nodeId );
        if ( nodeCache != null )
        {
            nodeCache.put( nodeId, node );
        }
        return node;
    }

    private RemoteNode getNode( long nodeId, boolean verified )
    {
        RemoteNode node = null;
        if ( nodeCache != null )
        {
            node = nodeCache.get( nodeId );
        }
        if ( node == null )
        {
            if ( verified || engine.hasNodeWithId( id, nodeId ) )
            {
                node = newNode( nodeId );
            }
            else
            {
                throw new NotFoundException(
                    "TODO: exception message. No node with id=" + nodeId );
            }
        }
        return node;
    }

    RemoteNode createNode()
    {
        return newNode( engine.createNode( id ) );
    }

    RemoteNode getNodeById( @SuppressWarnings( "hiding" ) long id )
    {
        return getNode( id, false );
    }

    RemoteNode getReferenceNode()
    {
        return getNode( engine.getReferenceNode( id ), true );
    }

    void deleteNode( RemoteNode node )
    {
        engine.deleteNode( id, node.id );
    }

    Iterable<Node> getAllNodes()
    {
        return new ConversionIterable<NodeSpecification, Node>( engine
            .getAllNodes( id ) )
        {
            @Override
            Node convert( NodeSpecification source )
            {
                return getNode( source.id, true );
            }
        };
    }

    Iterable<RelationshipType> getRelationshipTypes()
    {
        return new ConversionIterable<String, RelationshipType>( engine
            .getRelationshipTypes( id ) )
        {
            @Override
            RelationshipType convert( String source )
            {
                return engine.type( source );
            }
        };
    }

    // Relationship management

    private RemoteRelationship newRelationship( RelationshipSpecification spec )
    {
        RemoteRelationship relationship = new RemoteRelationship( engine,
            spec.relationshipId, engine.type( spec.name ), getNode(
                spec.startNodeId, true ), getNode( spec.endNodeId, true ) );
        if ( relationshipCache != null )
        {
            relationshipCache.put( spec.relationshipId, relationship );
        }
        return relationship;
    }

    private RemoteRelationship getRelationship( long relationshipId,
        RelationshipSpecification spec )
    {
        RemoteRelationship relationship = null;
        if ( relationshipCache != null )
        {
            relationship = relationshipCache.get( relationshipId );
        }
        if ( relationship == null )
        {
            if ( spec == null )
            {
                spec = engine.getRelationshipById( id, relationshipId );
            }
            relationship = newRelationship( spec );
        }
        return relationship;
    }

    RemoteRelationship createRelationship( RelationshipType type,
        RemoteNode startNode, RemoteNode endNode )
    {
        return newRelationship( engine.createRelationship( id, type.name(),
            startNode.id, endNode.id ) );
    }

    RemoteRelationship getRelationshipById( @SuppressWarnings( "hiding" ) long id )
    {
        return getRelationship( id, null );
    }

    Iterable<Relationship> getRelationships( RemoteNode node, Direction dir,
        RelationshipType... types )
    {
        final Iterable<RelationshipSpecification> spec;
        if ( types == null )
        {
            spec = engine.getAllRelationships( id, node.id, dir );
        }
        else
        {
            String[] typeNames = new String[ types.length ];
            for ( int i = 0; i < types.length; i++ )
            {
                typeNames[ i ] = types[ i ].name();
            }
            spec = engine.getRelationships( id, node.id, dir, typeNames );
        }
        return new ConversionIterable<RelationshipSpecification, Relationship>(
            spec )
        {
            @Override
            Relationship convert( RelationshipSpecification source )
            {
                return getRelationship( source.relationshipId, source );
            }
        };
    }

    void deleteRelationship( RemoteRelationship relationship )
    {
        engine.deleteRelationship( id, relationship.id );
    }

    // Traversal

    Iterable<TraversalPosition> traverse( RemoteNode startNode, Order order,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType[] relationshipTypes, Direction[] directions )
    {
        return engine.traverse( id, startNode, order, stopEvaluator,
            returnableEvaluator, relationshipTypes, directions );
    }

    // Property management

    Object getProperty( RemoteNode owner, String key )
    {
        return engine.getNodeProperty( id, owner.id, key );
    }

    Object getProperty( RemoteRelationship owner, String key )
    {
        return engine.getRelationshipProperty( id, owner.id, key );
    }

    void setProperty( RemoteNode owner, String key, Object value )
    {
        engine.setNodeProperty( id, owner.id, key, value );
    }

    void setProperty( RemoteRelationship owner, String key, Object value )
    {
        engine.setRelationshipProperty( id, owner.id, key, value );
    }

    Iterable<String> getPropertyKeys( RemoteNode owner )
    {
        return engine.getNodePropertyKeys( id, owner.id );
    }

    Iterable<String> getPropertyKeys( RemoteRelationship owner )
    {
        return engine.getRelationshipPropertyKeys( id, owner.id );
    }

    boolean hasProperty( RemoteNode owner, String key )
    {
        return engine.hasNodeProperty( id, owner.id, key );
    }

    boolean hasProperty( RemoteRelationship owner, String key )
    {
        return engine.hasRelationshipProperty( id, owner.id, key );
    }

    Object removeProperty( RemoteNode owner, String key )
    {
        return engine.removeNodeProperty( id, owner.id, key );
    }

    Object removeProperty( RemoteRelationship owner, String key )
    {
        return engine.removeRelationshipProperty( id, owner.id, key );
    }

    // Index operations

    IndexHits<Node> getIndexNodes( int indexId, String key, Object value )
    {
        BatchIterable<NodeSpecification> iter = engine.getIndexNodes( id,
            indexId, key, value );
        return new IndexHitsImpl( iter )
        {
            @Override
            Node convert( NodeSpecification source )
            {
                return getNode( source.id, true );
            }
        };
    }

    void indexNode( int indexId, RemoteNode node, String key, Object value )
    {
        engine.indexNode( id, indexId, node.id, key, value );
    }

    void removeIndexNode( int indexId, RemoteNode node, String key, Object value )
    {
        engine.removeIndexNode( id, indexId, node.id, key, value );
    }

    private static abstract class ConversionIterable<F, T> implements
        Iterable<T>
    {
        private final Iterable<F> source;

        ConversionIterable( Iterable<F> source )
        {
            this.source = source;
        }

        abstract T convert( @SuppressWarnings( "hiding" ) F source );

        public Iterator<T> iterator()
        {
            return new CloseableIterator<T>()
            {
                Iterator<F> iter = source.iterator();

                public boolean hasNext()
                {
                    return iter.hasNext();
                }

                public T next()
                {
                    return convert( iter.next() );
                }

                public void remove()
                {
                    iter.remove();
                }

                @SuppressWarnings( "unchecked" )
                public void close()
                {
                    if ( source instanceof CloseableIterator )
                    {
                        ( ( CloseableIterator ) source ).close();
                    }
                }
            };
        }
    }
    private static abstract class IndexHitsImpl implements IndexHits<Node>,
        CloseableIterator<Node>
    {
        private final CloseableIteratorWithSize<NodeSpecification> nodes;

        IndexHitsImpl( BatchIterable<NodeSpecification> nodes )
        {
            this.nodes = nodes.iterator();
        }

        abstract Node convert( NodeSpecification source );

        public final Iterator<Node> iterator()
        {
            return this;
        }

        @SuppressWarnings( "unchecked" )
        public final void close()
        {
            if ( nodes instanceof CloseableIterator )
            {
                ( ( CloseableIterator ) nodes ).close();
            }
        }

        public final int size()
        {
            return ( int ) nodes.size();
        }

        public final boolean hasNext()
        {
            return nodes.hasNext();
        }

        public final Node next()
        {
            return convert( nodes.next() );
        }

        public final void remove()
        {
            nodes.remove();
        }
    }
}
