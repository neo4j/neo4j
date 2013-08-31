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
package org.neo4j.graphdb;

import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;

public class QuickReadPerformanceTest
{
    private static final Timer timer = new Timer( "Background writer", /* daemon= */true );

    public static void main(String ... args) throws Exception
    {
        File storeDir = new File("/tmp/mydb");
        if ( storeDir.exists() )
        {
            FileUtils.deleteRecursively(storeDir);
        }
        final GraphDatabaseService db = new EmbeddedGraphDatabase( storeDir.getAbsolutePath() );

        try
        {
            Callable<int[]> reader = new BulkReaderWorker( db );
            startBackgroundInsert( db );

            long totalReads = 0;
            long totalTime = 0;
            long iterations = 30000;
            while(iterations --> 0)
            {
                int[] result = reader.call();
                totalReads += result[0];
                totalTime  += result[2];
            }

            System.out.println("Did " + totalReads + " in " + totalTime + "ms.");
            System.out.println(totalReads / totalTime + "reads/ms");

        }
        finally
        {
            timer.cancel();
            db.shutdown();
        }
    }

    private static void startBackgroundInsert( final GraphDatabaseService db )
    {
        timer.schedule( new TimerTask()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Node node1 = db.createNode();
                    Node node2 = db.createNode();

                    node1.setProperty( "name", "Bob" );
                    node2.setProperty( "name", "Ashton" );

                    Relationship rel = node1.createRelationshipTo( node2, DynamicRelationshipType.withName(
                            "LIKES" ) );
                    rel.setProperty( "since", 12 );

                    tx.success();
                }
            }
        }, 10, 200 );
    }

    static class BulkReaderWorker implements Callable<int[]>
    {

        private final GraphDatabaseService graphDb;

        public BulkReaderWorker( GraphDatabaseService graphDb )
        {
            this.graphDb = graphDb;
        }

        @Override
        public int[] call() throws Exception
        {
            int reads = 0;
            long time = System.currentTimeMillis();
            for ( int i = 0; i < 10; i++ )
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    for ( Node node : graphDb.getAllNodes() )
                    {
                        try
                        {
                            reads += 1;
                            for ( Relationship r : node.getRelationships() )
                            {
                                reads += 1;
                                for ( String propertyKey : r.getPropertyKeys() )
                                {
                                    r.getProperty( propertyKey );
                                    reads += 2; // Prop key and prop value
                                }
                            }
                            for ( String propertyKey : node.getPropertyKeys() )
                            {
                                node.getProperty( propertyKey );
                                reads += 2; // Prop key and prop value
                            }
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                            if ( e instanceof NullPointerException )
                            {
                                // most likely the database has been shutdown
                                throw e;
                            }
                        }
                    }
                }
            }

            int[] result = new int[3];
            result[0] = reads;
            result[1] = 0;
            result[2] = (int) ( System.currentTimeMillis() - time );
            return result;
        }
    }


}
