/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.graphalgo.benchmark.graphgeneration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.traversal.TraverserFactory;

/**
 * This class can be used to represent part of a network (like a subgraph),
 * represented as a set of nodes and a set of edges. All this really does is
 * filter the results of Node.getRelationships to only return relationships
 * within this subnetwork, thus limiting traversals and searches to the nodes
 * within the subnetwork. Therefore, any changes made to the subnetwork will be
 * reflected in the underlying network as well. The subgraph always starts out
 * empty, and can be emptied again with the clear() method. It can then be
 * filled with nodes and edges through the various methods supplied. This class
 * can of course also be used to retrieve the set of all nodes and the set of
 * all edges from a network.
 * @author Patrik Larsson
 */
public class SubNetwork
{
    Set<Relationship> subNetworkRelationships = new HashSet<Relationship>();
    Set<Node> subNetworkNodes = new HashSet<Node>();
    TraverserFactory traverserFactory = new TraverserFactory();

    /**
     * Adds a tree to this subnetwork by doing a breadth first search of a given
     * depth, adding all nodes found and the first edge leading to each node.
     * @param node
     *            The starting node
     * @param searchDepth
     *            The search depth.
     * @param relationshipType
     *            Relation type to traverse.
     * @param direction
     *            Direction in which to traverse relationships.
     */
    public void addTreeFromCentralNode( Node node, final int searchDepth,
        RelationshipType relationshipType, Direction direction )
    {
        Traverser traverser = node.traverse( Order.BREADTH_FIRST,
            new StopEvaluator()
            {
                public boolean isStopNode( TraversalPosition currentPos )
                {
                    return currentPos.depth() >= searchDepth;
                }
            }, ReturnableEvaluator.ALL, relationshipType, direction );
        for ( Node node2 : traverser )
        {
            subNetworkNodes.add( node2 );
            subNetworkRelationships.add( traverser.currentPosition()
                .lastRelationshipTraversed() );
        }
    }

    /**
     * Makes a search of a given depth from a given node and adds all found
     * nodes and relationships to this subnetwork.
     * @param node
     *            The starting node
     * @param searchDepth
     *            The search depth.
     * @param relationshipType
     *            Relation type to traverse.
     * @param direction
     *            Direction in which to traverse relationships.
     * @param includeBoundaryRelationships
     *            If false, relationships between nodes where the maximum depth
     *            has been reached will not be included since the search depth
     *            is considered to have been exhausted at them.
     */
    public void addSubNetworkFromCentralNode( Node node, final int searchDepth,
        RelationshipType relationshipType, Direction direction,
        boolean includeBoundaryRelationships )
    {
        internalAddSubNetworkFromCentralNode( node, searchDepth,
            relationshipType, direction, includeBoundaryRelationships,
            new HashMap<Node,Integer>() );
    }

    /**
     * Same as addSubNetworkFromCentralNode, but the internal version with some
     * extra data sent along.
     * @param nodeScanDepths
     *            This stores at what depth a certain node was added so we can
     *            ignore it when we reach it with a lower depth.
     */
    protected void internalAddSubNetworkFromCentralNode( Node node,
        final int searchDepth, RelationshipType relationshipType,
        Direction direction, boolean includeBoundaryRelationships,
        Map<Node,Integer> nodeScanDepths )
    {
        // We stop here if this node has already been scanned and we this time
        // have a "shorter" way to go beyond it.
        Integer previousDepth = nodeScanDepths.get( node );
        if ( previousDepth != null && previousDepth >= searchDepth )
        {
            return;
        }
        subNetworkNodes.add( node );
        nodeScanDepths.put( node, searchDepth );
        if ( searchDepth == 0 && includeBoundaryRelationships )
        {
            for ( Relationship relationship : node.getRelationships(
                relationshipType, direction ) )
            {
                if ( subNetworkNodes
                    .contains( relationship.getOtherNode( node ) ) )
                {
                    subNetworkRelationships.add( relationship );
                }
            }
        }
        if ( searchDepth <= 0 )
        {
            return;
        }
        for ( Relationship relationship : node.getRelationships(
            relationshipType, direction ) )
        {
            subNetworkRelationships.add( relationship );
            internalAddSubNetworkFromCentralNode( relationship
                .getOtherNode( node ), searchDepth - 1, relationshipType,
                direction, includeBoundaryRelationships, nodeScanDepths );
        }
    }

    public void clear()
    {
        subNetworkRelationships = new HashSet<Relationship>();
        subNetworkNodes = new HashSet<Node>();
    }

    protected Relationship filterRelationship( Relationship relationship )
    {
        if ( subNetworkRelationships.contains( relationship ) )
        {
            return relationship;
        }
        return null;
    }

    protected Iterable<Relationship> filterRelationships(
        Iterable<Relationship> rels )
    {
        List<Relationship> result = new LinkedList<Relationship>();
        for ( Relationship relationship : rels )
        {
            if ( filterRelationship( relationship ) != null )
            {
                result.add( relationship );
            }
        }
        return result;
    }

    class SubNetWorkNode implements Node
    {
        Node underlyingNode;

        public SubNetWorkNode( Node underlyingNode )
        {
            super();
            this.underlyingNode = underlyingNode;
        }

        /**
         * @param otherNode
         * @param type
         * @return
         * @see org.neo4j.api.core.Node#createRelationshipTo(org.neo4j.api.core.Node,
         *      org.neo4j.api.core.RelationshipType)
         */
        public Relationship createRelationshipTo( Node otherNode,
            RelationshipType type )
        {
            return new SubNetworkRelationship( underlyingNode
                .createRelationshipTo( otherNode, type ) );
        }

        /**
         * @see org.neo4j.api.core.Node#delete()
         */
        public void delete()
        {
            underlyingNode.delete();
        }

        /**
         * @return
         * @see org.neo4j.api.core.Node#getId()
         */
        public long getId()
        {
            return underlyingNode.getId();
        }

        /**
         * @param arg0
         * @param arg1
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getProperty(java.lang.String,
         *      java.lang.Object)
         */
        public Object getProperty( String arg0, Object arg1 )
        {
            return underlyingNode.getProperty( arg0, arg1 );
        }

        /**
         * @param arg0
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getProperty(java.lang.String)
         */
        public Object getProperty( String arg0 )
        {
            return underlyingNode.getProperty( arg0 );
        }

        /**
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getPropertyKeys()
         */
        public Iterable<String> getPropertyKeys()
        {
            return underlyingNode.getPropertyKeys();
        }

        /**
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getPropertyValues()
         */
        public Iterable<Object> getPropertyValues()
        {
            return underlyingNode.getPropertyValues();
        }

        /**
         * @return
         * @see org.neo4j.api.core.Node#getRelationships()
         */
        public Iterable<Relationship> getRelationships()
        {
            return filterRelationships( underlyingNode.getRelationships() );
        }

        /**
         * @param dir
         * @return
         * @see org.neo4j.api.core.Node#getRelationships(org.neo4j.api.core.Direction)
         */
        public Iterable<Relationship> getRelationships( Direction dir )
        {
            return filterRelationships( underlyingNode.getRelationships( dir ) );
        }

        /**
         * @param type
         * @param dir
         * @return
         * @see org.neo4j.api.core.Node#getRelationships(org.neo4j.api.core.RelationshipType,
         *      org.neo4j.api.core.Direction)
         */
        public Iterable<Relationship> getRelationships( RelationshipType type,
            Direction dir )
        {
            return filterRelationships( underlyingNode.getRelationships( type,
                dir ) );
        }

        /**
         * @param types
         * @return
         * @see org.neo4j.api.core.Node#getRelationships(org.neo4j.api.core.RelationshipType[])
         */
        public Iterable<Relationship> getRelationships(
            RelationshipType... types )
        {
            return filterRelationships( underlyingNode.getRelationships( types ) );
        }

        /**
         * @param type
         * @param dir
         * @return
         * @see org.neo4j.api.core.Node#getSingleRelationship(org.neo4j.api.core.RelationshipType,
         *      org.neo4j.api.core.Direction)
         */
        public Relationship getSingleRelationship( RelationshipType type,
            Direction dir )
        {
            return filterRelationship( underlyingNode.getSingleRelationship(
                type, dir ) );
        }

        /**
         * @param arg0
         * @return
         * @see org.neo4j.api.core.PropertyContainer#hasProperty(java.lang.String)
         */
        public boolean hasProperty( String arg0 )
        {
            return underlyingNode.hasProperty( arg0 );
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

        /**
         * @param arg0
         * @return
         * @see org.neo4j.api.core.PropertyContainer#removeProperty(java.lang.String)
         */
        public Object removeProperty( String arg0 )
        {
            return underlyingNode.removeProperty( arg0 );
        }

        /**
         * @param arg0
         * @param arg1
         * @see org.neo4j.api.core.PropertyContainer#setProperty(java.lang.String,
         *      java.lang.Object)
         */
        public void setProperty( String arg0, Object arg1 )
        {
            underlyingNode.setProperty( arg0, arg1 );
        }

        public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            RelationshipType relationshipType, Direction direction )
        {
            if ( direction == null )
            {
                throw new IllegalArgumentException( "Null direction" );
            }
            if ( relationshipType == null )
            {
                throw new IllegalArgumentException( "Null relationship type" );
            }
            // rest of parameters will be validated in traverser package
            return traverserFactory
                .createTraverser( traversalOrder, this, relationshipType,
                    direction, stopEvaluator, returnableEvaluator );
        }

        public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            RelationshipType firstRelationshipType, Direction firstDirection,
            RelationshipType secondRelationshipType, Direction secondDirection )
        {
            if ( firstDirection == null || secondDirection == null )
            {
                throw new IllegalArgumentException( "Null direction, "
                    + "firstDirection=" + firstDirection + "secondDirection="
                    + secondDirection );
            }
            if ( firstRelationshipType == null
                || secondRelationshipType == null )
            {
                throw new IllegalArgumentException( "Null rel type, "
                    + "first=" + firstRelationshipType + "second="
                    + secondRelationshipType );
            }
            // rest of parameters will be validated in traverser package
            RelationshipType[] types = new RelationshipType[2];
            Direction[] dirs = new Direction[2];
            types[0] = firstRelationshipType;
            types[1] = secondRelationshipType;
            dirs[0] = firstDirection;
            dirs[1] = secondDirection;
            return traverserFactory.createTraverser( traversalOrder, this,
                types, dirs, stopEvaluator, returnableEvaluator );
        }

        public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            Object... relationshipTypesAndDirections )
        {
            int length = relationshipTypesAndDirections.length;
            if ( (length % 2) != 0 || length == 0 )
            {
                throw new IllegalArgumentException( "Variable argument should "
                    + " consist of [RelationshipType,Direction] pairs" );
            }
            int elements = relationshipTypesAndDirections.length / 2;
            RelationshipType[] types = new RelationshipType[elements];
            Direction[] dirs = new Direction[elements];
            int j = 0;
            for ( int i = 0; i < elements; i++ )
            {
                Object relType = relationshipTypesAndDirections[j++];
                if ( !(relType instanceof RelationshipType) )
                {
                    throw new IllegalArgumentException(
                        "Expected RelationshipType at var args pos " + (j - 1)
                            + ", found " + relType );
                }
                types[i] = (RelationshipType) relType;
                Object direction = relationshipTypesAndDirections[j++];
                if ( !(direction instanceof Direction) )
                {
                    throw new IllegalArgumentException(
                        "Expected Direction at var args pos " + (j - 1)
                            + ", found " + direction );
                }
                dirs[i] = (Direction) direction;
            }
            return traverserFactory.createTraverser( traversalOrder, this,
                types, dirs, stopEvaluator, returnableEvaluator );
        }
    }
    public class SubNetworkRelationship implements Relationship
    {
        Relationship underlyingRelationship;

        public SubNetworkRelationship( Relationship underlyingRelationship )
        {
            super();
            this.underlyingRelationship = underlyingRelationship;
        }

        /**
         * @see org.neo4j.api.core.Relationship#delete()
         */
        public void delete()
        {
            underlyingRelationship.delete();
        }

        /**
         * @return
         * @see org.neo4j.api.core.Relationship#getEndNode()
         */
        public Node getEndNode()
        {
            return new SubNetWorkNode( underlyingRelationship.getEndNode() );
        }

        /**
         * @return
         * @see org.neo4j.api.core.Relationship#getId()
         */
        public long getId()
        {
            return underlyingRelationship.getId();
        }

        /**
         * @return
         * @see org.neo4j.api.core.Relationship#getNodes()
         */
        public Node[] getNodes()
        {
            return new Node[] { getStartNode(), getEndNode() };
        }

        /**
         * @param node
         * @return
         * @see org.neo4j.api.core.Relationship#getOtherNode(org.neo4j.api.core.Node)
         */
        public Node getOtherNode( Node node )
        {
            return new SubNetWorkNode( underlyingRelationship
                .getOtherNode( node ) );
        }

        /**
         * @param arg0
         * @param arg1
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getProperty(java.lang.String,
         *      java.lang.Object)
         */
        public Object getProperty( String arg0, Object arg1 )
        {
            return underlyingRelationship.getProperty( arg0, arg1 );
        }

        /**
         * @param arg0
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getProperty(java.lang.String)
         */
        public Object getProperty( String arg0 )
        {
            return underlyingRelationship.getProperty( arg0 );
        }

        /**
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getPropertyKeys()
         */
        public Iterable<String> getPropertyKeys()
        {
            return underlyingRelationship.getPropertyKeys();
        }

        /**
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getPropertyValues()
         */
        public Iterable<Object> getPropertyValues()
        {
            return underlyingRelationship.getPropertyValues();
        }

        /**
         * @return
         * @see org.neo4j.api.core.Relationship#getStartNode()
         */
        public Node getStartNode()
        {
            return new SubNetWorkNode( underlyingRelationship.getStartNode() );
        }

        /**
         * @return
         * @see org.neo4j.api.core.Relationship#getType()
         */
        public RelationshipType getType()
        {
            return underlyingRelationship.getType();
        }

        /**
         * @param arg0
         * @return
         * @see org.neo4j.api.core.PropertyContainer#hasProperty(java.lang.String)
         */
        public boolean hasProperty( String arg0 )
        {
            return underlyingRelationship.hasProperty( arg0 );
        }

        /**
         * @param type
         * @return
         * @see org.neo4j.api.core.Relationship#isType(org.neo4j.api.core.RelationshipType)
         */
        public boolean isType( RelationshipType type )
        {
            return underlyingRelationship.isType( type );
        }

        /**
         * @param arg0
         * @return
         * @see org.neo4j.api.core.PropertyContainer#removeProperty(java.lang.String)
         */
        public Object removeProperty( String arg0 )
        {
            return underlyingRelationship.removeProperty( arg0 );
        }

        /**
         * @param arg0
         * @param arg1
         * @see org.neo4j.api.core.PropertyContainer#setProperty(java.lang.String,
         *      java.lang.Object)
         */
        public void setProperty( String arg0, Object arg1 )
        {
            underlyingRelationship.setProperty( arg0, arg1 );
        }
    }
}
