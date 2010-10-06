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

package org.neo4j.remote;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

final class RemoteNode extends RemotePropertyContainer implements Node
{
    RemoteNode( RemoteGraphDbEngine txService, long id )
    {
        super( txService, id );
    }

    @Override
    public int hashCode()
    {
        return ( int ) id;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof RemoteNode )
        {
            RemoteNode node = ( RemoteNode ) obj;
            return node.id == id && node.engine.equals( engine );
        }
        else
        {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return "Node[" + id + "]";
    }

    public Relationship createRelationshipTo( Node otherNode,
        RelationshipType type )
    {
        if ( otherNode instanceof RemoteNode )
        {
            RemoteNode other = ( RemoteNode ) otherNode;
            if ( other.engine.equals( engine ) )
            {
                return engine.current().createRelationship( type, this, other );
            }
        }
        throw new IllegalArgumentException(
            "Other node not in same node space." );
    }

    public void delete()
    {
        engine.current().deleteNode( this );
    }

    public long getId()
    {
        return id;
    }

    public Iterable<Relationship> getRelationships()
    {
        return engine.current().getRelationships( this, Direction.BOTH,
            ( ( RelationshipType[] ) null ) );
    }

    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return engine.current().getRelationships( this, Direction.BOTH,
            ( types == null ? types : new RelationshipType[ 0 ] ) );
    }

    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return engine.current().getRelationships( this, dir,
            ( ( RelationshipType[] ) null ) );
    }

    public Iterable<Relationship> getRelationships( RelationshipType type,
        Direction dir )
    {
        RelationshipType[] types = { type };
        return engine.current().getRelationships( this, dir, types );
    }

    public Relationship getSingleRelationship( RelationshipType type,
        Direction dir )
    {
        Iterator<Relationship> relations = getRelationships( type, dir )
            .iterator();
        if ( !relations.hasNext() )
        {
            return null;
        }
        else
        {
            Relationship relation = relations.next();
            if ( relations.hasNext() )
            {
                throw new NotFoundException( "More then one relationship["
                    + type + "] found" );
            }
            return relation;
        }
    }

    public boolean hasRelationship()
    {
        return getRelationships().iterator().hasNext();
    }

    public boolean hasRelationship( RelationshipType... types )
    {
        return getRelationships( types ).iterator().hasNext();
    }

    public boolean hasRelationship( Direction dir )
    {
        return getRelationships( dir ).iterator().hasNext();
    }

    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return getRelationships( type, dir ).iterator().hasNext();
    }

    /* Tentative expansion API
    public Expansion<Relationship> expandAll()
    {
        return Traversal.expanderForAllTypes().expand( this );
    }

    public Expansion<Relationship> expand( RelationshipType type )
    {
        return expand( type, Direction.BOTH );
    }

    public Expansion<Relationship> expand( RelationshipType type,
            Direction direction )
    {
        return Traversal.expanderForTypes( type, direction ).expand(
                this );
    }

    public Expansion<Relationship> expand( Direction direction )
    {
        return Traversal.expanderForAllTypes( direction ).expand( this );
    }

    public Expansion<Relationship> expand( RelationshipExpander expander )
    {
        return Traversal.expander( expander ).expand( this );
    }
    */

    /*
     * NOTE: traversers are harder to build up remotely. Maybe traversal should
     * be done on the client side, using the regular primitive accessors.
     */
    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType relationshipType, Direction direction )
    {
        return traversal( traversalOrder, stopEvaluator, returnableEvaluator,
            new RelationshipType[] { relationshipType },
            new Direction[] { direction } );
    }

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType firstRelationshipType, Direction firstDirection,
        RelationshipType secondRelationshipType, Direction secondDirection )
    {
        return traversal( traversalOrder, stopEvaluator, returnableEvaluator,
            new RelationshipType[] { firstRelationshipType,
                secondRelationshipType }, new Direction[] { firstDirection,
                secondDirection } );
    }

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        Object... relationshipTypesAndDirections )
    {
        if ( relationshipTypesAndDirections.length % 2 != 0 )
        {
            throw new IllegalArgumentException(
                "Not as many directions as relationship types." );
        }
        RelationshipType[] relationshipTypes = new RelationshipType[ relationshipTypesAndDirections.length / 2 ];
        Direction[] directions = new Direction[ relationshipTypesAndDirections.length / 2 ];
        for ( int i = 0, j = 0; j < directions.length; i += 2, j++ )
        {
            try
            {
                relationshipTypes[ j ] = ( RelationshipType ) relationshipTypesAndDirections[ i ];
            }
            catch ( ClassCastException ex )
            {
                throw new IllegalArgumentException( "Not a RelationshipType: "
                    + relationshipTypesAndDirections[ i ] );
            }
            try
            {
                directions[ j ] = ( Direction ) relationshipTypesAndDirections[ i + 1 ];
            }
            catch ( ClassCastException ex )
            {
                throw new IllegalArgumentException( "Not a Direction: "
                    + relationshipTypesAndDirections[ i + 1 ] );
            }
        }
        return traversal( traversalOrder, stopEvaluator, returnableEvaluator,
            relationshipTypes, directions );
    }

    @Override
    Object getContainerProperty( String key )
    {
        return engine.current().getProperty( this, key );
    }

    public Iterable<String> getPropertyKeys()
    {
        return engine.current().getPropertyKeys( this );
    }

    public boolean hasProperty( String key )
    {
        return engine.current().hasProperty( this, key );
    }

    public Object removeProperty( String key )
    {
        return engine.current().removeProperty( this, key );
    }

    public void setProperty( String key, Object value )
    {
        engine.current().setProperty( this, key, value );
    }

    private Traverser traversal( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType[] relationshipTypes, Direction[] directions )
    {
        final Iterable<TraversalPosition> positions = engine.current()
            .traverse( this, traversalOrder, stopEvaluator,
                returnableEvaluator, relationshipTypes, directions );
        return new Traverser()
        {
            Iterator<TraversalPosition> iter = positions.iterator();
            TraversalPosition last = null;
            TraversalPosition current = null;

            public TraversalPosition currentPosition()
            {
                return last;
            }

            public Collection<Node> getAllNodes()
            {
                Collection<Node> result = new LinkedList<Node>();
                for ( Node node : this )
                {
                    result.add( node );
                }
                return result;
            }

            public Iterator<Node> iterator()
            {
                return new Iterator<Node>()
                {
                    public boolean hasNext()
                    {
                        if ( current != null )
                        {
                            return true;
                        }
                        else if ( iter.hasNext() )
                        {
                            current = iter.next();
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }

                    public Node next()
                    {
                        if ( hasNext() )
                        {
                            last = current;
                            current = null;
                            return last.currentNode();
                        }
                        else
                        {
                            throw new NoSuchElementException();
                        }
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
