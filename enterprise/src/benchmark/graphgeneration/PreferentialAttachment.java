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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.graphalgo.benchmark.graphgeneration.GraphStore.Graph;

public class PreferentialAttachment implements GeneratedGraph
{
    private final NeoService neo;
    RelationshipType relationshipType;
    int numberOfNodes;
    int minimumDegree;
    private long[] nodes;
    private final Random random;
    Transaction transaction;
    int transactionCount = 0;

    protected void renewTransaction()
    {
        if ( ++transactionCount > 1000 )
        {
            transactionCount = 0;
            transaction.success();
            transaction.finish();
            transaction = neo.beginTx();
        }
    }

    public Node getRandomNode( Node butNotThisOne )
    {
        while ( true )
        {
            long id = nodes[random.nextInt( nodes.length )];
            if ( butNotThisOne != null && id == butNotThisOne.getId() )
            {
                continue;
            }
            return neo.getNodeById( id );
        }
    }

    public Node getNodeByInternalId( long id )
    {
        return neo.getNodeById( nodes[(int) id] );
    }

    public Set<Relationship> getRelationships()
    {
        Set<Relationship> result = new HashSet<Relationship>();
        for ( long nodeId : nodes )
        {
            Node node = neo.getNodeById( nodeId );
            Iterable<Relationship> relationships = node
                .getRelationships( Direction.OUTGOING );
            for ( Relationship relationship : relationships )
            {
                result.add( relationship );
            }
        }
        return result;
    }

    public Set<Node> getNodes()
    {
        Set<Node> result = new HashSet<Node>();
        for ( long nodeId : nodes )
        {
            result.add( neo.getNodeById( nodeId ) );
        }
        return result;
    }

    public PreferentialAttachment( final NeoService neo, GraphStore graphStore,
        RelationshipType relationshipType, int numberOfNodes, int minimumDegree )
    {
        super();
        this.neo = neo;
        this.relationshipType = relationshipType;
        this.numberOfNodes = numberOfNodes;
        this.minimumDegree = minimumDegree;
        this.random = new Random( System.currentTimeMillis() );
        String ident = "PreferentialAttachment," + relationshipType + ","
            + numberOfNodes + "," + minimumDegree;
        if ( graphStore != null )
        {
            Graph graph = graphStore.loadGraph( ident );
            if ( graph != null )
            {
                System.out.println( "Loading graph" );
                this.nodes = graph.loadNodeIds();
            }
        }
        // Generate if needed
        if ( this.nodes == null )
        {
            generate();
            if ( graphStore != null )
            {
                graphStore.createGraph( ident ).saveNodeIds( nodes );
            }
        }
    }

    public void generate()
    {
        transaction = neo.beginTx();
        System.out.println( "Generating graph" );
        nodes = new long[numberOfNodes];
        ProgressBar progressBar = new ProgressBar( numberOfNodes
            * minimumDegree );
        int[] edges = new int[numberOfNodes * minimumDegree * 2];
        for ( int v = 0; v < numberOfNodes; ++v )
        {
            nodes[v] = neo.createNode().getId();
            renewTransaction();
            for ( int i = 0; i < minimumDegree; ++i )
            {
                if ( v == 0 )
                {
                    break;
                }
                // create an edge from the new node to some previous node, by
                // randomly choosing among all previous relationships (and both
                // ends of them)
                int offset = 2 * (v * minimumDegree + i);
                edges[offset] = v;
                // TODO: this right now does not choose among its own previous
                // relationships
                int j = random.nextInt( 2 * v * minimumDegree );
                edges[offset + 1] = edges[j];
                // make it so in the neo network
                Node node1 = neo.getNodeById( nodes[edges[offset]] );
                Node node2 = neo.getNodeById( nodes[edges[offset + 1]] );
                node1.createRelationshipTo( node2, relationshipType );
                renewTransaction();
                progressBar.Print();
            }
        }
        transaction.success();
        transaction.finish();
    }
}
