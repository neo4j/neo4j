/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package common;

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
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;

public class SimpleGraphBuilder
{
    public static final String KEY_ID = "name";

    GraphDatabaseService graphDb;
    Map<String,Node> nodes;
    Map<Node,String> nodeNames;
    Set<Relationship> edges;
    RelationshipType currentRelType;

    public SimpleGraphBuilder( GraphDatabaseService graphDb,
        RelationshipType relationshipType )
    {
        super();
        this.graphDb = graphDb;
        nodes = new HashMap<>();
        nodeNames = new HashMap<>();
        edges = new HashSet<>();
        setCurrentRelType( relationshipType );
    }

    public void clear()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            for ( Node node : nodes.values() )
            {
                node = transaction.getNodeById( node.getId() );
                for ( Relationship relationship : node.getRelationships() )
                {
                    relationship.delete();
                }
                node.delete();
            }
            nodes.clear();
            nodeNames.clear();
            edges.clear();
            transaction.commit();
        }
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

    public Node makeNode( Transaction tx, String id )
    {
        return makeNode( tx, id, Collections.emptyMap() );
    }

    public Node makeNode( Transaction tx, String id, Object... keyValuePairs )
    {
        return makeNode( tx, id, toMap( keyValuePairs ) );
    }

    private Map<String, Object> toMap( Object[] keyValuePairs )
    {
        Map<String, Object> map = new HashMap<>();
        for ( int i = 0; i < keyValuePairs.length; i++ )
        {
            map.put( keyValuePairs[i++].toString(), keyValuePairs[i] );
        }
        return map;
    }

    public Node makeNode( Transaction tx, String id, Map<String, Object> properties )
    {
        Node node = tx.createNode();
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

    public Node getNode( Transaction tx, String id )
    {
        return getNode( tx, id, false );
    }

    public Node getNode( Transaction tx, String id, boolean force )
    {
        Node node = nodes.get( id );
        if ( node == null && force )
        {
            node = makeNode( tx, id );
        }
        return node;
    }

    public String getNodeId( Node node )
    {
        return nodeNames.get( node );
    }

    public Relationship makeEdge( Transaction tx, String node1, String node2 )
    {
        return makeEdge( tx, node1, node2, Collections.emptyMap() );
    }

    public Relationship makeEdge( Transaction tx, String node1, String node2, Map<String, Object> edgeProperties )
    {
        Node n1 = getNode( tx, node1, true );
        Node n2 = getNode( tx, node2, true );
        Relationship relationship = n1
            .createRelationshipTo( n2, currentRelType );
        for ( Map.Entry<String, Object> property : edgeProperties.entrySet() )
        {
            relationship.setProperty( property.getKey(), property.getValue() );
        }
        edges.add( relationship );
        return relationship;
    }

    public Relationship makeEdge( Transaction transaction, String node1, String node2, Object... keyValuePairs )
    {
        return makeEdge( transaction, node1, node2, toMap( keyValuePairs ) );
    }

    /**
     * This creates a chain by adding a number of edges. Example: The input
     * string "a,b,c,d,e" makes the chain a->b->c->d->e
     * @param transaction
     * @param commaSeparatedNodeNames
     */
    public void makeEdgeChain( Transaction transaction, String commaSeparatedNodeNames )
    {
        String[] nodeNames = commaSeparatedNodeNames.split( "," );
        for ( int i = 0; i < nodeNames.length - 1; ++i )
        {
            makeEdge( transaction, nodeNames[i], nodeNames[i + 1] );
        }
    }

    /**
     * Same as makeEdgeChain, but with some property set on all edges.
     * @param transaction
     * @param commaSeparatedNodeNames
     *            A string with the node names separated by commas.
     * @param propertyName
     * @param propertyValue
     */
    public void makeEdgeChain( Transaction transaction, String commaSeparatedNodeNames, String propertyName, Object propertyValue )
    {
        String[] nodeNames = commaSeparatedNodeNames.split( "," );
        for ( int i = 0; i < nodeNames.length - 1; ++i )
        {
            makeEdge( transaction, nodeNames[i], nodeNames[i + 1], propertyName,
                propertyValue );
        }
    }

    /**
     * This creates a number of edges from a number of node names, pairwise.
     * Example: Input "a,b,c,d" gives a->b and c->d
     * @param transaction
     * @param commaSeparatedNodeNames
     */
    public void makeEdges( Transaction transaction, String commaSeparatedNodeNames )
    {
        String[] nodeNames = commaSeparatedNodeNames.split( "," );
        for ( int i = 0; i < nodeNames.length / 2; ++i )
        {
            makeEdge( transaction, nodeNames[i * 2], nodeNames[i * 2 + 1] );
        }
    }

    /**
     * @param node1Id
     * @param node2Id
     * @return One relationship between two given nodes, if there exists one,
     *         otherwise null.
     */
    public Relationship getRelationship( Transaction tx, String node1Id, String node2Id )
    {
        Node node1 = getNode( tx, node1Id );
        Node node2 = getNode( tx, node2Id );
        if ( node1 == null || node2 == null )
        {
            return null;
        }
        ResourceIterable<Relationship> relationships = Iterables.asResourceIterable( node1.getRelationships() );
        try ( ResourceIterator<Relationship> resourceIterator = relationships.iterator() )
        {
            while ( resourceIterator.hasNext() )
            {
                Relationship relationship = resourceIterator.next();
                if ( relationship.getOtherNode( node1 ).equals( node2 ) )
                {
                    return relationship;
                }
            }
        }
        return null;
    }

    // Syntax: makePathWithRelProperty( "weight", "a-4-b-2.3-c-3-d" )
    public Path makePathWithRelProperty( Transaction tx, String relPropertyName, String dashSeparatedNodeNamesAndRelationshipProperty )
    {
        String[] nodeNamesAndRelationshipProperties = dashSeparatedNodeNamesAndRelationshipProperty.split( "-" );
        Node startNode = getNode( tx, nodeNamesAndRelationshipProperties[0], true);
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
            Relationship relationship = makeEdge( tx, from, to, relPropertyName, prop );
            builder = builder.push( relationship );
        }
        return builder.build();
    }
}
