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
package org.neo4j.kernel.impl.core;

import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcessTestRunner;

/*
 * The purpose of this test case is to check that when transactions are received and applied from an instance
 * (prominent example is an HA slave) then the transaction becomes visible as a unit and not in parts. It does
 * that by executing transactions on one database and extracting them and applying them on another.
 * Initially it creates a node and a property on it (one tx) and then in another tx it removes the property.
 * When applied to the target database, it should not be possible to get InvalidRecordException by reading
 * the version of the node from the previous tx (with the property still attached) while in the new tx the
 * property record has already been deleted. This may happen because as of this writing property commands
 * are executed before node commands.
 */
@ForeignBreakpoints({@ForeignBreakpoints.BreakpointDef(type = "org.neo4j.kernel.impl.nioneo.xa.Command$NodeCommand",
        method = "execute", on = BreakPoint.Event.ENTRY),
        @ForeignBreakpoints.BreakpointDef(type = "org.neo4j.kernel.impl.nioneo.xa.WriteTransaction",
                method = "commitRecovered", on = BreakPoint.Event.ENTRY)})
@RunWith(SubProcessTestRunner.class)
@Ignore("This test case fails because no fix has been committed for it")
public class TestTxApplicationSynchronization
{
    private EmbeddedGraphDatabase baseDb;
    private EmbeddedGraphDatabase targetDb;
    private long nodeId;

    @Before
    public void before() throws Exception
    {
        TargetDirectory dir = TargetDirectory.forTest( getClass() );
        baseDb = new EmbeddedGraphDatabase( dir.directory( "base", true ).getAbsolutePath() );

        Transaction tx = baseDb.beginTx();
        Node node = baseDb.createNode();
        nodeId = node.getId();
        node.setProperty( "propName", "propValue" );
        tx.success();
        tx.finish();

        targetDb = new EmbeddedGraphDatabase( dir.directory( "target", true ).getAbsolutePath() );
        Pair<Long, ReadableByteChannel> lastTx = getLatestCommitedTx( baseDb );
        NeoStoreXaDataSource targetNeoDatasource = targetDb.getXaDataSourceManager().getNeoStoreDataSource();
        targetNeoDatasource.applyCommittedTransaction( lastTx.first(), lastTx.other() );
    }

    @Test
    @EnabledBreakpoints({"commitRecovered", "waitForSuspend", "resumeAll"})
    public void test() throws Exception
    {
        Transaction tx = baseDb.beginTx();
        baseDb.getNodeById( nodeId ).removeProperty( "propName" );
        tx.success();
        tx.finish();

        final CountDownLatch localLatch = new CountDownLatch( 1 );

        Thread updatePuller = new Thread( new Runnable()
        {
            public void run()
            {
                try
                {
                    Pair<Long, ReadableByteChannel> lastTx = getLatestCommitedTx( baseDb );
                    NeoStoreXaDataSource targetNeoDatasource = targetDb.getXaDataSourceManager()
                            .getNeoStoreDataSource();
                    /*
                     * To see this test case pass move the latch under the applyCommitedTransaction (so that tx
                     * application in this thread finishes first) and comment out the @EnabledBreakpoints above (so
                     * that synchronization is not messed with or it will deadlock).
                     */
                    localLatch.countDown();
                    targetNeoDatasource.applyCommittedTransaction( lastTx.first(), lastTx.other() );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        }, "writer" );

        updatePuller.start();
        Thread.sleep( 100 ); // I don't know why this is necessary
        localLatch.await(); // Wait for tx apply to start
        waitForSuspend(); // Wait for tx apply breakpoint to trigger
        targetDb.getNodeById( nodeId ).getProperty( "propName" ); // Get the exception
        resumeAll(); // Restart all threads
        updatePuller.join(); // Join so we don't leave stuff hanging
    }

    @BreakpointTrigger("waitForSuspend")
    private void waitForSuspend()
    {
    }

    @BreakpointTrigger("resumeAll")
    private void resumeAll()
    {
    }

    private static DebuggedThread updater;
    private static final CountDownLatch latch = new CountDownLatch( 1 );

    @BreakpointHandler("waitForSuspend")
    public static void suspendHandler( BreakPoint self, DebugInterface di ) throws Exception
    {
        latch.await();
    }

    @BreakpointHandler("resumeAll")
    public static void resumeAllHandler( BreakPoint self, DebugInterface di )
    {
        if ( updater != null )
        {
            updater.resume();
        }
    }

    @BreakpointHandler("commitRecovered")
    public static void handleCommitRecovered( BreakPoint self, DebugInterface di,
                                              @BreakpointHandler("execute") BreakPoint commandExecute )
    {
        if ( self.invocationCount() > 1 )
        {
            commandExecute.enable();
        }
    }

    @BreakpointHandler("execute")
    public static void handleExecute( BreakPoint self, DebugInterface di )
    {
        updater = di.thread();
        updater.suspend( null );
        latch.countDown();
    }

    private static Pair<Long, ReadableByteChannel> getLatestCommitedTx( EmbeddedGraphDatabase db ) throws Exception
    {
        XaDataSource neoDatasource = db.getXaDataSourceManager().getNeoStoreDataSource();
        long lastCommittedTxId = neoDatasource.getLastCommittedTxId();
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();

        neoDatasource.getLogExtractor( lastCommittedTxId, lastCommittedTxId ).extractNext( buffer );
        return Pair.of( lastCommittedTxId, (ReadableByteChannel) buffer );
    }
}
