/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.lastOrNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.index.lucene.QueryContext;

@Ignore( "These are performance and sanity tests which can't really be executed as automated tests " +
        "for example a long running test which checks so that files aren't kept open" )
public class TestPerformanceAndSanity extends AbstractLuceneIndexTest
{
    @Test
    public void testNodeInsertionSpeed()
    {
        testInsertionSpeed( nodeIndex( "insertion-speed",
                LuceneIndexImplementation.EXACT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void testNodeFulltextInsertionSpeed()
    {
        testInsertionSpeed( nodeIndex( "insertion-speed-full",
                LuceneIndexImplementation.FULLTEXT_CONFIG ), NODE_CREATOR );
    }

    @Test
    public void testRelationshipInsertionSpeed()
    {
        testInsertionSpeed( relationshipIndex( "insertion-speed",
                LuceneIndexImplementation.EXACT_CONFIG ), new FastRelationshipCreator() );
    }

    private <T extends PropertyContainer> void testInsertionSpeed(
            Index<T> index,
            EntityCreator<T> creator )
    {
        long t = currentTimeMillis();
        int max = 500000;
        for ( int i = 0; i < max; i++ )
        {
            T entity = creator.create();
            if ( i % 5000 == 5 )
            {
                index.query( new TermQuery( new Term( "name", "The name " + i ) ) );
            }
            lastOrNull( (Iterable<T>) index.query( new QueryContext( new TermQuery( new Term( "name", "The name " + i ) ) ).tradeCorrectnessForSpeed() ) );
            lastOrNull( (Iterable<T>) index.get( "name", "The name " + i ) );
            index.add( entity, "name", "The name " + i );
            index.add( entity, "title", "Some title " + i );
            index.add( entity, "something", i + "Nothing" );
            index.add( entity, "else", i + "kdfjkdjf" + i );
            if ( i % 30000 == 0 )
            {
                restartTx();
                System.out.println( i );
            }
        }
        finishTx( true );
        out.println( "insert:" + ( currentTimeMillis() - t ) );

        t = currentTimeMillis();
        int count = 2000000;
        int resultCount = 0;
        for ( int i = 0; i < count; i++ )
        {
            resultCount += count( (Iterator<T>) index.get( "name", "The name " + i%max ) );
        }
        out.println( "get(" + resultCount + "):" + (double)( currentTimeMillis() - t ) / (double)count );

        t = currentTimeMillis();
        resultCount = 0;
        for ( int i = 0; i < count; i++ )
        {
            resultCount += count( (Iterator<T>) index.get( "something", i%max + "Nothing" ) );
        }
        out.println( "get(" + resultCount + "):" + (double)( currentTimeMillis() - t ) / (double)count );
    }

    /**
     * Starts multiple threads which updates and queries an index concurrently
     * during a long period of time just to make sure that number of file handles doesn't grow.
     * @throws Exception
     */
    @Test
    public void makeSureFilesAreClosedProperly() throws Exception
    {
        commitTx();
        final Index<Node> index = nodeIndex( "open-files", LuceneIndexImplementation.EXACT_CONFIG );
        final long time = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch( 30 );
        for ( int t = 0; t < latch.getCount(); t++ ) 
        {
            final int thread = t;
            new Thread()
            {
                public void run()
                {
                    for ( int i = 0; System.currentTimeMillis() - time < 60*1000*10; i++ )
                    {
                        if ( i%10 == 0 )
                        {
                            if ( i%100 == 0 )
                            {
                                int size = 0;
                                int type = (int)(System.currentTimeMillis()%3);
                                if ( type == 0 )
                                {
                                    IndexHits<Node> itr = index.get( "key", "value5" );
                                    try
                                    {
                                        itr.getSingle();
                                    }
                                    catch ( NoSuchElementException e )
                                    { // For when there are multiple hits
                                    }
                                    size = 99;
                                }
                                else if ( type == 1 )
                                {
                                    IndexHits<Node> itr = index.get( "key", "value5" );
                                    for ( ;itr.hasNext() && size < 5; size++ )
                                    {
                                        itr.next();
                                    }
                                    itr.close();
                                }
                                else
                                {
                                    IndexHits<Node> itr = index.get( "key", "crap value" ); /* Will return 0 hits */
                                    // Iterate over the hits sometimes (it's always gonna be 0 sized)
                                    if ( System.currentTimeMillis()%10 > 5 )
                                    {
                                        IteratorUtil.count( (Iterator<Node>) itr );
                                    }
                                }
                            }
                            else
                            {
                                int size = IteratorUtil.count( (Iterator<Node>) index.get( "key", "value5" ) );
                                if ( thread == 0 )
                                {
                                    System.out.println( "hit size:" + size );
                                }
                            }
                        }
                        else
                        {
                            Transaction tx = graphDb.beginTx();
                            try
                            {
                                for ( int ii = 0; ii < 20; ii++ )
                                {
                                    Node node = graphDb.createNode();
                                    index.add( node, "key", "value" + ii );
                                }
                                tx.success();
                            }
                            finally
                            {
                                tx.finish();
                            }
                        }
                    }
                    latch.countDown();
                }
            }.start();
        }
        latch.await();
    }

    @Test
    public void testPerformanceForManySmallTransactions() throws Exception
    {
        final Index<Node> index = nodeIndex( "index", LuceneIndexImplementation.EXACT_CONFIG );
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
                            Transaction tx = graphDb.beginTx();
                            try
                            {
                                for ( int ii = 0; ii < group; ii++ )
                                {
                                    Node node = graphDb.createNode();
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
    }
}
