/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class TestSmallTransactions
{
    @Ignore
    @Test
    public void testPerformanceForManySmallTransactions() throws Exception
    {
        String path = "target/var/performance";
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
        final GraphDatabaseService db = new EmbeddedGraphDatabase( path );
        final Index<Node> index = db.index().forNodes( "index" );
        final int count = 5000;
        final int group = 1;
        final int threads = 3;
        final Collection<Thread> threadList = new ArrayList<Thread>();
        final AtomicInteger id = new AtomicInteger();
        final AtomicBoolean halt = new AtomicBoolean();
        long t = System.currentTimeMillis();
        
        for ( int h = 0; h < threads; h++ )
        {
            final int threadId = h;
            Thread thread = new Thread()
            {
                public void run()
                {
                    try
                    {
                        for ( int i = 0; i < count; i+=group )
                        {
                            if ( halt.get() ) break;
                            Transaction tx = db.beginTx();
                            try
                            {
                                for ( int ii = 0; ii < group; ii++ )
                                {
                                    Node node = db.createNode();
                                    index.get( "key", "value" + System.currentTimeMillis()%count ).getSingle();
                                    index.add( node, "key", "value" + id.getAndIncrement() );
                                }
                                tx.success();
                            }
                            finally
                            {
                                tx.finish();
                            }
                            if ( i%100 == 0 ) System.out.println( threadId + ": " + i );
                        }
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace( System.out );
                        halt.set( true );
                    }
                }
            };
            threadList.add( thread );
            thread.start();
        }
        for ( Thread aThread : threadList )
        {
            aThread.join();
        }
        long t1 = System.currentTimeMillis()-t;
        
//        System.out.println( "2" );
//        t = System.currentTimeMillis();
//        for ( int i = 0; i < count; i++ )
//        {
//            Transaction tx = db.beginTx();
//            try
//            {
//                Node node = index.get( "key", "value" + i ).getSingle();
//                node.setProperty( "something", "sjdkjk" );
//                tx.success();
//            }
//            finally
//            {
//                tx.finish();
//            }
//            if ( i%100 == 0 && i > 0 ) System.out.println( i );
//        }
//        long t2 = System.currentTimeMillis()-t;
        
        System.out.println( t1 + ", " + (double)t1/(double)count );
        
        db.shutdown();
    }
}
