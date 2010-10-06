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

public class SmallWorldGraph implements GeneratedGraph
{
    private long[] nodes;
    private final NeoService neo;
    RelationshipType relationshipType;
    int numberOfNodes;
    int numberOfRelationshipsFromEachNode;
    private final Random random;
    Transaction transaction;
    int transactionCount = 0;
    double rewireFactor;
    boolean rewireReplaces = true;
    boolean distanceRelativeRewiring = false;

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

    /**
     * @param neo
     * @param graphStore
     * @param relationshipType
     * @param numberOfNodes
     * @param numberOfRelationshipsFromEachNode
     *            The out-degree. I.e. the average node degree will be twice
     *            this number.
     * @param rewireFactor
     *            Probability for each edge to be connected to some "far-away"
     *            node.
     * @param rewireReplaces
     *            If true, long distance connections replaces neighbor
     *            connections. If false neighbor connections are also made.
     * @param distanceRelativeRewiring
     *            If true, long distance edges are draw with probability
     *            depending on distance. If false, edges are made with uniform
     *            probability.
     */
    public SmallWorldGraph( NeoService neo, GraphStore graphStore,
        RelationshipType relationshipType, int numberOfNodes,
        int numberOfRelationshipsFromEachNode, double rewireFactor,
        boolean rewireReplaces, boolean distanceRelativeRewiring )
    {
        super();
        this.neo = neo;
        this.numberOfNodes = numberOfNodes;
        this.numberOfRelationshipsFromEachNode = numberOfRelationshipsFromEachNode;
        this.relationshipType = relationshipType;
        this.rewireFactor = rewireFactor;
        this.rewireReplaces = rewireReplaces;
        this.distanceRelativeRewiring = distanceRelativeRewiring;
        this.random = new Random( System.currentTimeMillis() );
        String ident = "SmallWorldGraph," + relationshipType + ","
            + numberOfNodes + "," + numberOfRelationshipsFromEachNode + ","
            + rewireFactor + "," + rewireReplaces + ","
            + distanceRelativeRewiring;
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
            Generate();
            if ( graphStore != null )
            {
                graphStore.createGraph( ident ).saveNodeIds( nodes );
            }
        }
    }

    public void Generate()
    {
        transaction = neo.beginTx();
        System.out.println( "Generating graph" );
        nodes = new long[numberOfNodes];
        ProgressBar progressBar = new ProgressBar( numberOfNodes );
        for ( int n = 0; n < numberOfNodes; ++n )
        {
            nodes[n] = neo.createNode().getId();
            renewTransaction();
            progressBar.Print();
        }
        progressBar = new ProgressBar( numberOfNodes );
        for ( int n = 0; n < numberOfNodes; ++n )
        {
            Node node = getNodeByInternalId( n );
            for ( int i = 0; i < numberOfRelationshipsFromEachNode; ++i )
            {
                Node node2 = null;
                if ( random.nextDouble() < rewireFactor )
                {
                    if ( !distanceRelativeRewiring )
                    {
                        node2 = getRandomNode( node );
                    }
                    else
                    {
                        // this generates a random k where 1 <= k <=
                        // numberOfNodes/2-1
                        double r = random.nextDouble()
                            * Math.log( numberOfNodes / 2 );
                        int k = (int) Math.exp( r );
                        node2 = getNodeByInternalId( (n + k)
                            % (numberOfNodes / 2) );
                    }
                }
                if ( node2 != null )
                {
                    node.createRelationshipTo( node2, relationshipType );
                    renewTransaction();
                }
                if ( node2 == null || !rewireReplaces )
                {
                    node2 = getNodeByInternalId( (n + i + 1) % numberOfNodes );
                    node.createRelationshipTo( node2, relationshipType );
                    renewTransaction();
                }
            }
            progressBar.Print();
        }
        transaction.success();
        transaction.finish();
    }
}
