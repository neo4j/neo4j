/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.qa.tooling.DumpProcessInformation;
import org.neo4j.test.EmbeddedDatabaseRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.DebuggerDeadlockCallback;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcess;
import org.neo4j.test.subprocess.SubProcessTestRunner;

import static java.util.concurrent.TimeUnit.MINUTES;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.Predicates.stringContains;
import static org.neo4j.qa.tooling.DumpVmInformation.dumpVmInfo;

@ForeignBreakpoints( {
                      @ForeignBreakpoints.BreakpointDef( type = "org.neo4j.kernel.impl.core.ArrayBasedPrimitive",
                              method = "setProperties" ),
                      @ForeignBreakpoints.BreakpointDef( type = "org.neo4j.kernel.impl.core.NodeManager",
                              method = "getNodeIfCached" ) } )
@RunWith( SubProcessTestRunner.class )
@Ignore( "Ignored in 2.0 due to half-way refactoring moving properties into kernel API. " +
         "Unignore and change appropriately when it's done" )
public class TestPropertyDataRace
{
    @ClassRule
    public static EmbeddedDatabaseRule database = new EmbeddedDatabaseRule();

    public static final TargetDirectory targetDir = TargetDirectory.forTest( TestPropertyDataRace.class );

    @Test
    @EnabledBreakpoints( { "enable breakpoints", "done" } )
    public void readingMutatorVersusCommittingMutator() throws Exception
    {
        final Node one, two;
        final GraphDatabaseService graphdb = database.getGraphDatabaseService();
        Transaction tx = graphdb.beginTx();
        try
        {
            one = graphdb.createNode();
            two = graphdb.createNode();
            one.setProperty( "node", "one" );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
        clearCaches();
        final CountDownLatch done = new CountDownLatch( 2 ), prepare = new CountDownLatch( 1 );
        new Thread( "committing mutator" )
        {
            @Override
            public void run()
            {
                Transaction txn = graphdb.beginTx();
                try
                {
                    for ( String key : one.getPropertyKeys() )
                    {
                        one.removeProperty( key );
                    }
                    clearCaches();
                    prepare.countDown();

                    txn.success();
                }
                finally
                {
                    txn.finish();
                }
                txn = graphdb.beginTx();
                try
                {
                    two.setProperty( "node", "two" );

                    txn.success();
                }
                finally
                {
                    txn.finish();
                }
                countDown( done );
            }
        }.start();
        new Thread( "reading mutator" )
        {
            @Override
            public void run()
            {
                Transaction txn = graphdb.beginTx();
                try
                {
                    while ( true )
                    {
                        try
                        {
                            prepare.await();
                            break;
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.interrupted(); // reset
                        }
                    }
                    for ( String key : one.getPropertyKeys() )
                    {
                        one.removeProperty( key );
                    }

                    txn.success();
                }
                finally
                {
                    txn.finish();
                }
                clearCaches();
                done.countDown();
            }
        }.start();

        if ( !done.await( 1, MINUTES ) )
        {
            File dumpDirectory = targetDir.cleanDirectory( "dump" );
            dumpVmInfo( dumpDirectory );
            new DumpProcessInformation( new SystemOutLogging(), dumpDirectory ).doThreadDump(
                    stringContains( SubProcess.class.getSimpleName() ) );
            fail( "Test didn't complete within a reasonable time, dumping process information to " + dumpDirectory );
        }

        for ( String key : two.getPropertyKeys() )
        {
            assertEquals( "two", two.getProperty( key ) );
        }
    }

    @BreakpointTrigger( "enable breakpoints" )
    private void clearCaches()
    {
        database.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( Caches.class ).clear();
    }

    @BreakpointTrigger( "done" )
    private void countDown( CountDownLatch latch )
    {
        latch.countDown();
    }

    private static DebuggedThread thread;
    private static final DebuggerDeadlockCallback RESUME_THREAD = new DebuggerDeadlockCallback()
    {
        @Override
        public void deadlock( DebuggedThread thread )
        {
            // Another thread wants to get into the synchronized region,
            // time for the sleeping thread in there to make progress
            thread.resume();
        }
    };

    @BreakpointHandler( "enable breakpoints" )
    public static void onEnableBreakpoints( BreakPoint self,
                                            @BreakpointHandler( "getNodeIfCached" ) BreakPoint getNodeIfCached,
                                            @BreakpointHandler( "setProperties" ) BreakPoint setProperties )
    {
        if ( getNodeIfCached.isEnabled() )
        {
            setProperties.enable();
            self.disable();
        }
        else
        {
            getNodeIfCached.enable();
        }
    }

    @BreakpointHandler( "setProperties" )
    public static void onSetProperties( BreakPoint self, DebugInterface di )
    {
        self.disable();
        if ( thread != null )
        {
            thread.resume();
        }
        thread = di.thread().suspend( RESUME_THREAD );
    }

    @BreakpointHandler( "getNodeIfCached" )
    public static void onGetNodeIfCached( BreakPoint self, DebugInterface di )
    {
        self.disable();
        if ( thread == null )
        {
            thread = di.thread().suspend( null );
        }
    }

    @BreakpointHandler( "done" )
    public static void onDone()
    {
        thread.resume();
        thread = null;
    }
}
