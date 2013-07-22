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
package org.neo4j.kernel.impl.core;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcess;
import org.neo4j.test.subprocess.SubProcessTestRunner;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.Predicates.stringContains;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.qa.tooling.DumpProcessInformation.doThreadDump;
import static org.neo4j.qa.tooling.DumpVmInformation.dumpVmInfo;
import static org.neo4j.test.subprocess.DebuggerDeadlockCallback.RESUME_THREAD;

/**
 * This test tests the exact same issue as {@link TestConcurrentModificationOfRelationshipChains}. The difference is
 * that it tries to cut it as close as possible by doing the relationship cache load right after the removal of the
 * relationship from the cache. This causes the node to be loaded with wrong information about the next relationship
 * since it has not been deleted on disk yet. It is purely a cache thing and dealt with by invalidation of deleted
 * relationships on commit instead of on prepare.
 */
@ForeignBreakpoints( {
        @ForeignBreakpoints.BreakpointDef( type = "org.neo4j.kernel.impl.nioneo.xa.WriteTransaction",
                method = "doPrepare", on = BreakPoint.Event.EXIT ) } )
@RunWith( SubProcessTestRunner.class )
public class TestRelationshipConcurrentDeleteAndLoadCachePoisoning
{
    private static final int RelationshipGrabSize = 2;

    @ClassRule
    public static EmbeddedDatabaseRule database = new EmbeddedDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.relationship_grab_size, "" + RelationshipGrabSize );
        }
    };

    public static final TargetDirectory targetDir =
            TargetDirectory.forTest( TestRelationshipConcurrentDeleteAndLoadCachePoisoning.class );

    private static DebuggedThread committer;
    private static DebuggedThread reader;

    @Test
    @EnabledBreakpoints( {"doPrepare", "waitForPrepare", "readDone"} )
    public void theTest() throws Exception
    {
        final GraphDatabaseAPI db = database.getGraphDatabaseAPI();

        Transaction tx = db.beginTx();
        final Node first = db.createNode();
        final Relationship theOneAfterTheGap =
                first.createRelationshipTo( db.createNode(), DynamicRelationshipType.withName( "AC" ) );
        // The gap
        for ( int i = 0; i < RelationshipGrabSize; i++)
        {
            first.createRelationshipTo( db.createNode(), DynamicRelationshipType.withName( "AC" ) );
        }
        tx.success();
        tx.finish();

        // This is required, otherwise relChainPosition is never consulted, everything will already be in mem.
        db.getDependencyResolver().resolveDependency( NodeManager.class ).clearCache();

        Runnable writer = new Runnable()
        {
            @Override
            public void run()
            {
                Transaction tx = db.beginTx();
                theOneAfterTheGap.delete();
                tx.success();
                tx.finish();
            }
        };

        Runnable reader = new Runnable()
        {
            @Override
            public void run()
            {
                waitForPrepare();
                // Get the first batch into the cache - relChainPosition points to theOneAfterTheGap
                Transaction transaction = db.beginTx();
                first.getRelationships().iterator().next();
                transaction.finish();
                readDone();
            }
        };

        Thread writerThread = new Thread( writer );
        Thread readerThread = new Thread( reader );

        // Start order matters - suspend the reader first, then start the writes.
        readerThread.start();
        writerThread.start();

        dumpAndFailIfNotDeadWithin( readerThread, 1, MINUTES );
        dumpAndFailIfNotDeadWithin( writerThread, 1, MINUTES );

        // This should pass without any problems.
        Transaction transaction = db.beginTx();
        int count = count( first.getRelationships() );
        assertEquals( "Should have read relationships created minus one", RelationshipGrabSize, count );
        transaction.finish();
    }

    private void dumpAndFailIfNotDeadWithin( Thread thread, int duration, TimeUnit unit ) throws Exception
    {
        thread.join( MILLISECONDS.convert( duration, unit ) );
        if ( thread.isAlive() )
        {
            File dumpDirectory = targetDir.directory( "dump", true );
            dumpVmInfo( dumpDirectory );
            doThreadDump( stringContains( SubProcess.class.getSimpleName() ), dumpDirectory );
            fail( "Test didn't complete within a reasonable time, dumping process information to " + dumpDirectory );
        }
    }

    @BreakpointHandler( "doPrepare" )
    public static void onDoPrepare( BreakPoint self, DebugInterface di )
    {
        if ( self.invocationCount() < 3 )
        {
            // One for the rel type, one for the setup
            return;
        }
        self.disable();
        committer = di.thread();
        committer.suspend( RESUME_THREAD );
        reader.resume();
    }

    @BreakpointTrigger("waitForPrepare")
    public void waitForPrepare()
    {
    }

    @BreakpointHandler( "waitForPrepare" )
    public static void onWaitForPrepare( BreakPoint self, DebugInterface di )
    {
        self.disable();
        reader = di.thread();
        reader.suspend( RESUME_THREAD );
    }

    @BreakpointTrigger("readDone")
    public void readDone()
    {
    }

    @BreakpointHandler( "readDone" )
    public static void onReadDone( BreakPoint self, DebugInterface di )
    {
        self.disable();
        committer.resume();
    }
}
