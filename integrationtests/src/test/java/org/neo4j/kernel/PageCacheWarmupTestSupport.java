/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmerMonitor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;

class PageCacheWarmupTestSupport
{
    void createTestData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Label label = Label.label( "Label" );
            RelationshipType relationshipType = RelationshipType.withName( "REL" );
            long[] largeValue = new long[1024];
            for ( int i = 0; i < 1000; i++ )
            {
                Node node = db.createNode( label );
                node.setProperty( "Niels", "Borh" );
                node.setProperty( "Albert", largeValue );
                for ( int j = 0; j < 30; j++ )
                {
                    Relationship rel = node.createRelationshipTo( node, relationshipType );
                    rel.setProperty( "Max", "Planck" );
                }
            }
            tx.success();
        }
    }

    long waitForCacheProfile( GraphDatabaseAPI db )
    {
        AtomicLong pageCount = new AtomicLong();
        BinaryLatch profileLatch = new BinaryLatch();
        PageCacheWarmerMonitor listener = new AwaitProfileMonitor( pageCount, profileLatch );
        Monitors monitors = db.getDependencyResolver().resolveDependency( Monitors.class );
        monitors.addMonitorListener( listener );
        profileLatch.await();
        monitors.removeMonitorListener( listener );
        return pageCount.get();
    }

    BinaryLatch pauseProfile( GraphDatabaseAPI db )
    {
        Monitors monitors = db.getDependencyResolver().resolveDependency( Monitors.class );
        return new PauseProfileMonitor( monitors );
    }

    private static class AwaitProfileMonitor implements PageCacheWarmerMonitor
    {
        private final AtomicLong pageCount;
        private final BinaryLatch profileLatch;

        AwaitProfileMonitor( AtomicLong pageCount, BinaryLatch profileLatch )
        {
            this.pageCount = pageCount;
            this.profileLatch = profileLatch;
        }

        @Override
        public void warmupCompleted( long pagesLoaded )
        {
        }

        @Override
        public void profileCompleted( long pagesInMemory )
        {
            pageCount.set( pagesInMemory );
            profileLatch.release();
        }
    }

    private static class PauseProfileMonitor extends BinaryLatch implements PageCacheWarmerMonitor
    {
        private final Monitors monitors;

        PauseProfileMonitor( Monitors monitors )
        {
            this.monitors = monitors;
            monitors.addMonitorListener( this );
        }

        @Override
        public void warmupCompleted( long pagesLoaded )
        {
        }

        @Override
        public void profileCompleted( long pagesInMemory )
        {
            await();
            monitors.removeMonitorListener( this );
        }
    }
}
