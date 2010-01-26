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
package org.neo4j.graphalgo.testUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class SimpleGraphBuilder
{
    public static final String KEY_ID = "__simpleGraphBuilderId__";
    
    GraphDatabaseService neo;
    HashMap<String,Node> nodes;
    HashMap<Node,String> nodeNames;
    Set<Relationship> edges;
    RelationshipType currentRelType = null;

    public SimpleGraphBuilder( GraphDatabaseService graphDb,
        RelationshipType relationshipType )
    {
        super();
        this.neo = graphDb;
        nodes = new HashMap<String,Node>();
        nodeNames = new HashMap<Node,String>();
        edges = new HashSet<Relationship>();
        setCurrentRelType( relationshipType );
    }

    public void clear()
    {
        for ( Node node : nodes.values() )
        {
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }
            node.delete();
        }
        nodes.clear();
        nodeNames.clear();
        edges.clear();
    }

    public Set<Relationship> getAllEdges()
    {
        return edges;
    }

    public Set<Node> getAllNodes()
    {
        return nodeNames.keySet();
    }

    public void setCurrentRelType( RelationshipType currentRelType )
    {
        this.currentRelType = currentRelType;
    }

    public Node makeNode( String id )
    {
        Node node = neo.createNode();
        nodes.put( id, node );
        nodeNames.put( node, id );
        node.setProperty( KEY_ID, id );
        return node;
    }

    public Node getNode( String id )
    {
        return getNode( id, false );
    }

    public Node getNode( String id, boolean force )
    {
        Node node = nodes.get( id );
        if ( node == null && force )
        {
            node = makeNode( id );
        }
        return node;
    }

    public String getNodeId( Node node )
    {
        return nodeNames.get( node );
    }

    public Relationship makeEdge( String node1, String node2 )
    {
        Node n1 = getNode( node1, true ), n2 = getNode( node2, true );
        Relationship relationship = n1
            .createRelationshipTo( n2, currentRelType );
        edges.add( relationship );
        return relationship;
    }

    public Relationship makeEdge( String node1, String node2,
        String propertyName, Object propertyValue )
    {
        Relationship relationship = makeEdge( node1, node2 );
        relationship.setProperty( propertyName, propertyValue );
        return relationship;
    }

    /**
     * This creates a chain by adding a number of edges. Example: The input
     * string "a,b,c,d,e" makes the chain a->b->c->d->e
     * @param commaSeparatedNodeNames
     *            A string with the node names separated by commas.
     */
    public void makeEdgeChain( String commaSeparatedNodeNames )
    {
        String[] nodeNames = commaSeparatedNodeNames.split( "," );
        for ( int i = 0; i < nodeNames.length - 1; ++i )
        {
            makeEdge( nodeNames[i], nodeNames[i + 1] );
        }
    }

    /**
     * Same as makeEdgeChain, but with some property set on all edges.
     * @param commaSeparatedNodeNames
     *            A string with the node names separated by commas.
     * @param propertyName
     * @param propertyValue
     */
    public void makeEdgeChain( String commaSeparatedNodeNames,
        String propertyName, Object propertyValue )
    {
        String[] nodeNames = commaSeparatedNodeNames.split( "," );
        for ( int i = 0; i < nodeNames.length - 1; ++i )
        {
            makeEdge( nodeNames[i], nodeNames[i + 1], propertyName,
                propertyValue );
        }
    }

    /**
     * This creates a number of edges from a number of node names, pairwise.
     * Example: Input "a,b,c,d" gives a->b and c->d
     * @param commaSeparatedNodeNames
     */
    public void makeEdges( String commaSeparatedNodeNames )
    {
        String[] nodeNames = commaSeparatedNodeNames.split( "," );
        for ( int i = 0; i < nodeNames.length / 2; ++i )
        {
            makeEdge( nodeNames[i * 2], nodeNames[i * 2 + 1] );
        }
    }

    /**
     * Same as makeEdges, but with some property set on all edges.
     * @param commaSeparatedNodeNames
     * @param propertyName
     * @param propertyValue
     */
    public void makeEdges( String commaSeparatedNodeNames, String propertyName,
        Object propertyValue )
    {
        String[] nodeNames = commaSeparatedNodeNames.split( "," );
        for ( int i = 0; i < nodeNames.length / 2; ++i )
        {
            makeEdge( nodeNames[i * 2], nodeNames[i * 2 + 1], propertyName,
                propertyValue );
        }
    }

    /**
     * @param node1Id
     * @param node2Id
     * @return One relationship between two given nodes, if there exists one,
     *         otherwise null.
     */
    public Relationship getRelationship( String node1Id, String node2Id )
    {
        Node node1 = getNode( node1Id );
        Node node2 = getNode( node2Id );
        if ( node1 == null || node2 == null )
        {
            return null;
        }
        Iterable<Relationship> relationships = node1.getRelationships();
        for ( Relationship relationship : relationships )
        {
            if ( relationship.getOtherNode( node1 ).equals( node2 ) )
            {
                return relationship;
            }
        }
        return null;
    }
}
