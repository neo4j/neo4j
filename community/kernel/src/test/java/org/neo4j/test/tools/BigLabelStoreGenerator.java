package org.neo4j.test.tools;

import java.util.Arrays;
import java.util.Random;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static java.lang.String.format;

// Builds a store in the path given as first argument of of -DnumNodes nodes, where each node has at most -DnumLabels
// randomly selected labels
public class BigLabelStoreGenerator
{
    private static Random random = new Random();

    public static void main(String[] args)
    {
        long batchSize = 1000;

        long numNodes = Long.parseLong( System.getProperty( "numNodes" ) );
        int numLabels = Integer.parseInt( System.getProperty( "numLabels" ) );

        Label[] labels = createLabels( numLabels );

        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseService graph = factory.newEmbeddedDatabase( args[0] );

        try {
            for ( long l = 0; l < numNodes; l += batchSize )
            {
                long labelings = 0;
                long start = System.currentTimeMillis();
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
                long duration = System.currentTimeMillis() - start;
                System.out.println( format( "nodes: %d, labelings: %d, duration: %d", l, labelings, duration ) );
            }
        }
        finally
        {
            graph.shutdown();
        }

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
