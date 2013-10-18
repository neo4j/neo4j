/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.test.tools;

import java.util.Arrays;
import java.util.Random;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

// Builds a store in the path at $GRAPH_DB of $NUM_NODES nodes, where each node has at most
// $NUM_LABELS randomly selected labels
public class BigLabelStoreGenerator
{
    private static Random random = new Random();

    public static void main(String[] args)
    {
        long batchSize = parseLong( withDefault( System.getenv().get( "BATCH_SIZE" ), "100000" ) );
        long numNodes = parseLong( withDefault( System.getenv( "NUM_NODES" ), "1000000" ) );
        int numLabels = parseInt( withDefault( System.getenv( "NUM_LABELS" ), "10" ) );

        Label[] labels = createLabels( numLabels );

        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseService graph = factory.newEmbeddedDatabase( System.getenv( "GRAPH_DB" ) );
        long labelings = 0;

        long start = System.currentTimeMillis();
        try {
            for ( long l = 0; l < numNodes; l += batchSize )
            {
                long batchStart = System.currentTimeMillis();
                try ( Transaction tx = graph.beginTx() )
                {
                    for ( long m = 0; m < batchSize; m++ )
                    {
                        Label[] selectedLabels = pickRandomLabels( labels );
                        labelings += selectedLabels.length;
                        graph.createNode( selectedLabels );
                    }
                    tx.success();
                }
                long batchDuration = System.currentTimeMillis() - batchStart;
                System.out.println( format( "nodes: %d, ratio: %d, labelings: %d, duration: %d", l, l*100/numNodes, labelings, batchDuration ) );
            }
        }
        finally
        {
            graph.shutdown();
        }
        long duration = System.currentTimeMillis() - start;
        System.out.println( format( "nodes: %d, ratio: %d, labelings: %d, duration: %d", numNodes, 100, labelings, duration ) );
    }

    private static String withDefault( String value, String defaultValue )
    {
        return null == value ? defaultValue : value;
    }

    private static Label[] pickRandomLabels( Label[] labels )
    {
        return Arrays.copyOf( labels, 1 + random.nextInt( labels.length - 1 ) );
    }

    private static Label[] createLabels( int numLabels )
    {
        Label[] labels = new DynamicLabel[numLabels];
        for ( int i = 0; i < numLabels; i++ )
        {
            labels[i] = DynamicLabel.label( format( "LABEL_%d", i ) );
        }
        return labels;
    }
}
