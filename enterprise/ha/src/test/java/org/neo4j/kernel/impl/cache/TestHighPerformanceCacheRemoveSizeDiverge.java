/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcessTestRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

@ForeignBreakpoints( {
    @ForeignBreakpoints.BreakpointDef( type = "org.neo4j.kernel.impl.cache.AutoLoadingCache", method = "updateSize" )
} )
@RunWith( SubProcessTestRunner.class )
public class TestHighPerformanceCacheRemoveSizeDiverge
{
    private static GraphDatabaseAPI graphdb;
    private static DebuggedThread thread;
    private static CountDownLatch latch = new CountDownLatch( 1 );

    @BeforeClass
    public static void startDb()
    {
        try
        {
        graphdb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( forTest(TestHighPerformanceCacheRemoveSizeDiverge.class ).makeGraphDbDir(
                ).getAbsolutePath()).
                setConfig( stringMap( GraphDatabaseSettings.cache_type.name(), HighPerformanceCacheProvider.NAME ) ).
                newGraphDatabase();
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
        }
    }

    @AfterClass
    public static void shutdownDb()
    {
        try
        {
            if ( graphdb != null ) graphdb.shutdown();
        }
        finally
        {
            graphdb = null;
        }
    }

    private Node createNodeWithSomeRelationships()
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Node node = graphdb.createNode();
            for ( int i = 0; i < 10; i++ )
                node.createRelationshipTo( node, MyRelTypes.TEST );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }
    
    @BreakpointHandler( "updateSize" )
    public static void onUpdateSize( BreakPoint self, DebugInterface di )
    {
        self.disable();
        thread = di.thread().suspend( null );
        latch.countDown();
    }
    
    @BreakpointHandler( "resumeUpdateSize" )
    public static void onResumeUpdateSize( BreakPoint self, DebugInterface di )
    {
        thread.resume();
    }
    
    @BreakpointTrigger( "resumeUpdateSize" )
    private void resumeUpdateSize() {}
    
    @BreakpointTrigger( "enableBreakpoints" )
    private void enableBreakpoints() {}
    
    @BreakpointHandler( "enableBreakpoints" )
    public static void onEnableBreakpoints( @BreakpointHandler( "updateSize" ) BreakPoint updateSize, DebugInterface di )
    {
        updateSize.enable();
    }
    
    @Test
    @EnabledBreakpoints( { "enableBreakpoints", "resumeUpdateSize" } )
    public void removeFromCacheInBetweenOtherThreadStateChangeAndUpdateSize() throws Exception
    {   // ...should yield a size which matches actual cache size
        
        /* Here's the English version of how to trigger it:
         * T1: create node N with 10 relationships
         * T1: clear cache (simulating that it needs to be loaded for the first time the next access)
         * T1: request N so that it gets put into cache (no relationships loaded)
         * T1: load relationships of N, break right before call to NodeImpl#updateSize
         * T2: remove N from cache, which calls N.size() which will return a size different from
         *     what the cache thinks that object is so it will subtract more than it should.
         * T1: resume execution
         * 
         * =>  cache size should be 0, but the bug makes it less than zero. Over time the cache will
         *     diverge more and more from actual cache size.
         */

        final Node node = createNodeWithSomeRelationships();
        graphdb.getDependencyResolver().resolveDependency( Caches.class ).clear();
        enableBreakpoints();
        Transaction transaction = graphdb.beginTx();
        try
        {
            graphdb.getNodeById( node.getId() );
        }
        finally
        {
            transaction.finish();
        }
        final Cache<?> nodeCache = graphdb.getDependencyResolver().resolveDependency( Caches.class ).node();
        assertTrue( "We didn't get a hold of the right cache object", nodeCache.getName().toLowerCase().contains( "node" ) );

        Thread t1 = new Thread( "T1: Relationship loader" )
        {
            @Override
            public void run()
            {
                // It will break in NodeImpl#loadInitialRelationships right before calling updateSize
                Transaction transaction = graphdb.beginTx();
                try
                {
                    count( node.getRelationships() );
                }
                finally
                {
                    transaction.finish();
                }
            }
        };
        t1.start();
        // TODO wait for latch instead, but it seems to be a different instance than the one we countDown.
//        latch.await();
        Thread.sleep( 2000 );
        
        Thread t2 = new Thread( "T2: Cache remover" )
        {
            @Override
            public void run()
            {
                nodeCache.remove( node.getId() );
            }
        };
        t2.start();
        t2.join();
        
        resumeUpdateSize();
        t1.join();
        
        assertEquals( "Invalid cache size for " + nodeCache, 0, nodeCache.size() );
    }

}
