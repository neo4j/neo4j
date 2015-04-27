/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

public class TerminateTransactions
{
    private static final String DB_PATH = "target/neo4j-terminate-tx-db";

    public static void main(String[] args) throws IOException
    {

        System.out.println( new TerminateTransactions().run() );
    }

    public String run() throws IOException
    {
        FileUtils.deleteRecursively( new File( DB_PATH ) );

        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        // END SNIPPET: startDb

        // START SNIPPET: mkTree
        RelationshipType relType = DynamicRelationshipType.withName( "CHILD" );
        Queue<Node> nodes = new LinkedList<>();
        int depth = 1;

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node rootNode = graphDb.createNode();
            nodes.add( rootNode );

            // END SNIPPET: mkTree
            Terminator terminator = new Terminator(tx);
            terminator.terminateAfter( 1000 );

            // START SNIPPET: mkTree
            for (; true; depth++) {
                int nodesToExpand = nodes.size();
                for (int i = 0; i < nodesToExpand; ++i) {
                    Node parent = nodes.remove();

                    Node left = graphDb.createNode();
                    Node right = graphDb.createNode();

                    parent.createRelationshipTo( left, relType );
                    parent.createRelationshipTo( right, relType );

                    nodes.add( left );
                    nodes.add( right );
                }
            }
        }
        catch ( TransactionTerminatedException ignored )
        {
            return String.format( "Created tree up to depth %s in 1 sec", depth );
        }
        // END SNIPPET: mkTree
        finally
        {
            // START SNIPPET: shutdownDb
            graphDb.shutdown();
            // END SNIPPET: shutdownDb
        }
    }

    public class Terminator
    {
        private final Transaction tx;

        Terminator( Transaction tx ) {
            this.tx = tx;
        }

        public void terminateAfter( final long millis )
        {
            Executors.newSingleThreadExecutor().submit( new Runnable()
            {
                @Override
                public void run()
                {
                    long startTime = System.currentTimeMillis();
                    do
                    {
                        try
                        {
                            Thread.sleep( millis );
                        }
                        catch ( InterruptedException ignored )
                        {
                            // terminated while sleeping
                        }
                    }
                    while ( (System.currentTimeMillis() - startTime) < millis );

                    // START SNIPPET: terminateTx
                    tx.terminate();
                    // END SNIPPET: terminateTx
                }
            } );
        }
    }


}
