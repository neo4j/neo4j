/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
 * This class can be used to generate an in-memory copy of a part of a network
 * (like a subgraph), represented as a set of copys of nodes and a set of copys
 * of edges. Currently, any changes made to the subnetwork will be reflected in
 * the underlying network as well. The subgraph always starts out empty, and can
 * be emptied again with the clear() method. It can then be filled with nodes
 * and edges through the various methods supplied. This class can of course be
 * used to retrieve the set of all nodes and the set of all edges from a
 * network.
 * @author Patrik Larsson
 */
public class SubNetworkCopy
{
    Set<Relationship> subNetworkRelationships = new HashSet<Relationship>();
    Map<Node,SubNetworkNode> nodeMap = new HashMap<Node,SubNetworkNode>();

    public SubNetworkRelationship addRelationship(
        Relationship underlyingRelationship )
    {
        SubNetworkRelationship subNetworkRelationship = new SubNetworkRelationship(
            underlyingRelationship, nodeMap.get( underlyingRelationship
                .getStartNode() ), nodeMap.get( underlyingRelationship
                .getEndNode() ) );
        subNetworkRelationships.add( subNetworkRelationship );
        return subNetworkRelationship;
    }

    /**
     * Empties this subnetwork.
     */
    public void clear()
    {
        subNetworkRelationships = new HashSet<Relationship>();
        nodeMap = new HashMap<Node,SubNetworkNode>();
    }

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
     *            Direction in which to traverse edges.
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
            nodeMap.put( node2, new SubNetworkNode( node2 ) );
            Relationship relationship = traverser.currentPosition()
                .lastRelationshipTraversed();
            if ( relationship != null )
            {
                addRelationship( relationship );
            }
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
     *            Direction in which to traverse edges.
     */
    public void addSubNetworkFromCentralNode( Node node, final int searchDepth,
        RelationshipType relationshipType, Direction direction )
    {
        internalAddSubNetworkFromCentralNode( node, searchDepth,
            relationshipType, direction, new HashMap<Node,Integer>() );
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
        Direction direction, Map<Node,Integer> nodeScanDepths )
    {
        // We stop here if this node has already been scanned and we this time
        // have a "shorter" way to go beyond it.
        Integer previousDepth = nodeScanDepths.get( node );
        if ( previousDepth != null && previousDepth >= searchDepth )
        {
            return;
        }
        nodeMap.put( node, new SubNetworkNode( node ) );
        nodeScanDepths.put( node, searchDepth );
        for ( Relationship relationship : node.getRelationships(
            relationshipType, direction ) )
        {
            Node otherNode = relationship.getOtherNode( node );
            if ( nodeMap.containsKey( otherNode ) )
            {
                addRelationship( relationship );
            }
        }
        if ( searchDepth <= 0 )
        {
            return;
        }
        for ( Relationship relationship : node.getRelationships(
            relationshipType, direction ) )
        {
            internalAddSubNetworkFromCentralNode( relationship
                .getOtherNode( node ), searchDepth - 1, relationshipType,
                direction, nodeScanDepths );
        }
    }

    /**
     * @return the relationships
     */
    public Set<Relationship> getEdges()
    {
        return subNetworkRelationships;
    }

    /**
     * @return the nodes
     */
    public Set<Node> getNodes()
    {
        return new HashSet<Node>( nodeMap.values() );
    }

    TraverserFactory traverserFactory = new TraverserFactory();

    public class SubNetworkNode implements Node
    {
        Node underlyingNode;
        Map<RelationshipType,List<SubNetworkRelationship>> relationships = new HashMap<RelationshipType,List<SubNetworkRelationship>>();

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
         * @return
         * @see org.neo4j.api.core.PropertyContainer#getProperty(java.lang.String)
         */
        public Object getProperty( String arg0 )
        {
            return underlyingNode.getProperty( arg0 );
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
         * @param arg0
         * @return
         * @see org.neo4j.api.core.PropertyContainer#hasProperty(java.lang.String)
         */
        public boolean hasProperty( String arg0 )
        {
            return underlyingNode.hasProperty( arg0 );
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

        public SubNetworkNode( Node underlyingNode )
        {
            super();
            this.underlyingNode = underlyingNode;
        }

        public Relationship createRelationshipTo( Node otherNode,
            RelationshipType type )
        {
            SubNetworkNode otherSubNetworkNode = (SubNetworkNode) otherNode;
            Node otherUnderlyingNode = otherNode;
            // If otherNode is a subNetworkNode (of this network)
            if ( nodeMap.containsValue( otherNode ) )
            {
                otherUnderlyingNode = otherSubNetworkNode.underlyingNode;
            }
            // Otherwise add it
            // TODO: or throw some error?
            else
            {
                otherSubNetworkNode = new SubNetworkNode( otherUnderlyingNode );
                nodeMap.put( otherUnderlyingNode, otherSubNetworkNode );
            }
            Relationship underlyingRelationship = underlyingNode
                .createRelationshipTo( otherUnderlyingNode, type );
            return addRelationship( underlyingRelationship );
        }

        public void delete()
        {
            // TODO Auto-generated method stub
        }

        public Iterable<Relationship> getRelationships()
        {
            List<Relationship> result = new LinkedList<Relationship>();
            for ( RelationshipType relationshipType : relationships.keySet() )
            {
                result.addAll( relationships.get( relationshipType ) );
            }
            return result;
        }

        public Iterable<Relationship> getRelationships(
            RelationshipType... types )
        {
            List<Relationship> result = new LinkedList<Relationship>();
            for ( RelationshipType relationshipType : types )
            {
                result.addAll( relationships.get( relationshipType ) );
            }
            return result;
        }

        protected List<Relationship> getRelationshipsOfDirection(
            Iterable<Relationship> rels, Direction dir )
        {
            List<Relationship> result = new LinkedList<Relationship>();
            for ( Relationship relationship : rels )
            {
                if ( dir.equals( Direction.BOTH )
                    || (dir.equals( Direction.OUTGOING ) && relationship
                        .getStartNode().equals( this ))
                    || (dir.equals( Direction.INCOMING ) && relationship
                        .getEndNode().equals( this )) )
                {
                    result.add( relationship );
                }
            }
            return result;
        }

        protected boolean containsRelationshipOfDirection(
            Iterable<Relationship> rels, Direction dir )
        {
            for ( Relationship relationship : rels )
            {
                if ( dir.equals( Direction.BOTH )
                    || (dir.equals( Direction.OUTGOING ) && relationship
                        .getStartNode().equals( this ))
                    || (dir.equals( Direction.INCOMING ) && relationship
                        .getEndNode().equals( this )) )
                {
                    return true;
                }
            }
            return false;
        }

        public Iterable<Relationship> getRelationships( Direction dir )
        {
            return getRelationshipsOfDirection( getRelationships(), dir );
        }

        public Iterable<Relationship> getRelationships( RelationshipType type,
            Direction dir )
        {
            List<Relationship> result = new LinkedList<Relationship>();
            result.addAll( relationships.get( type ) );
            return result;
        }

        public Relationship getSingleRelationship( RelationshipType type,
            Direction dir )
        {
            List<SubNetworkRelationship> rels = relationships.get( type );
            if ( rels == null || rels.isEmpty() )
            {
                return null;
            }
            return rels.get( 0 );
        }

        public boolean hasRelationship()
        {
            return !relationships.isEmpty();
        }

        public boolean hasRelationship( RelationshipType... types )
        {
            for ( RelationshipType relationshipType : types )
            {
                List<SubNetworkRelationship> rels = relationships
                    .get( relationshipType );
                if ( rels != null && !rels.isEmpty() )
                {
                    return true;
                }
            }
            return false;
        }

        public boolean hasRelationship( Direction dir )
        {
            return containsRelationshipOfDirection( getRelationships(), dir );
        }

        public boolean hasRelationship( RelationshipType type, Direction dir )
        {
            return containsRelationshipOfDirection( getRelationships( type ),
                dir );
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
        Node startNode;
        Node endNode;

        public SubNetworkRelationship( Relationship underlyingRelationship,
            Node startNode, Node endNode )
        {
            super();
            this.underlyingRelationship = underlyingRelationship;
            this.startNode = startNode;
            this.endNode = endNode;
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

        public void delete()
        {
            // TODO Auto-generated method stub
        }

        public Node getEndNode()
        {
            return endNode;
        }

        public Node[] getNodes()
        {
            return new Node[] { startNode, endNode };
        }

        public Node getOtherNode( Node node )
        {
            if ( node.equals( startNode ) )
            {
                return endNode;
            }
            if ( node.equals( endNode ) )
            {
                return startNode;
            }
            throw new RuntimeException( "Node[" + node.getId()
                + "] not connected to this relationship[" + getId() + "]" );
        }

        public Node getStartNode()
        {
            return startNode;
        }
    }
}
