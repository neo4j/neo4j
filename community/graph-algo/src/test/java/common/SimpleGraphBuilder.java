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
package common;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class SimpleGraphBuilder
{
    public static final String KEY_ID = "name";
    
    GraphDatabaseService graphDb;
    HashMap<String,Node> nodes;
    HashMap<Node,String> nodeNames;
    Set<Relationship> edges;
    RelationshipType currentRelType = null;

    public SimpleGraphBuilder( GraphDatabaseService graphDb,
        RelationshipType relationshipType )
    {
        super();
        this.graphDb = graphDb;
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
        return makeNode( id, Collections.<String, Object>emptyMap() );
    }
    
    public Node makeNode( String id, Object... keyValuePairs )
    {
        return makeNode( id, toMap( keyValuePairs ) );
    }
    
    private Map<String, Object> toMap( Object[] keyValuePairs )
    {
        Map<String, Object> map = new HashMap<String, Object>();
        for ( int i = 0; i < keyValuePairs.length; i++ )
        {
            map.put( keyValuePairs[i++].toString(), keyValuePairs[i] );
        }
        return map;
    }

    public Node makeNode( String id, Map<String, Object> properties )
    {
        Node node = graphDb.createNode();
        nodes.put( id, node );
        nodeNames.put( node, id );
        node.setProperty( KEY_ID, id );
        for ( Map.Entry<String, Object> property : properties.entrySet() )
        {
            if ( property.getKey().equals( KEY_ID ) )
            {
                throw new RuntimeException( "Can't use '" + property.getKey() + "'" );
            }
            node.setProperty( property.getKey(), property.getValue() );
        }
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
        return makeEdge( node1, node2, Collections.<String, Object>emptyMap() );
    }
    
    public Relationship makeEdge( String node1, String node2, Map<String, Object> edgeProperties )
    {
        Node n1 = getNode( node1, true ), n2 = getNode( node2, true );
        Relationship relationship = n1
            .createRelationshipTo( n2, currentRelType );
        for ( Map.Entry<String, Object> property : edgeProperties.entrySet() )
        {
            relationship.setProperty( property.getKey(), property.getValue() );
        }
        edges.add( relationship );
        return relationship;
    }

    public Relationship makeEdge( String node1, String node2, Object... keyValuePairs )
    {
        return makeEdge( node1, node2, toMap( keyValuePairs ) );
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
    
    public void importEdges( File file )
    {
        try
        {
            CsvFileReader reader = new CsvFileReader( file );
            while ( reader.hasNext() )
            {
                String[] line = reader.next();
                makeEdge( line[0], line[1] );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
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

    // Syntax: makePathWithRelProperty( "weight", "a-4-b-2.3-c-3-d" )
    public Path makePathWithRelProperty( String relPropertyName, String dashSeparatedNodeNamesAndRelationshipProperty )
    {
        String[] nodeNamesAndRelationshipProperties = dashSeparatedNodeNamesAndRelationshipProperty.split( "-" );
        Node startNode = getNode( nodeNamesAndRelationshipProperties[0], true);
        PathImpl.Builder builder = new PathImpl.Builder( startNode );

        if ( nodeNamesAndRelationshipProperties.length < 1 )
        {
            return builder.build();
        }

        for ( int i = 0; i < nodeNamesAndRelationshipProperties.length - 2; i += 2 )
        {
            String from = nodeNamesAndRelationshipProperties[i];
            String to = nodeNamesAndRelationshipProperties[i + 2];
            String prop = nodeNamesAndRelationshipProperties[i + 1];
            Relationship relationship = makeEdge( from, to, relPropertyName, prop );
            builder = builder.push( relationship );
        }
        return builder.build();
    }
}
