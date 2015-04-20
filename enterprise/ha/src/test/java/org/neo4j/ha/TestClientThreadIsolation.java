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
package org.neo4j.ha;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakPoint.Event;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcessTestRunner;

@ForeignBreakpoints({@ForeignBreakpoints.BreakpointDef(type = "org.neo4j.com.Client",
        method = "makeSureNextTransactionIsFullyFetched", on = Event.ENTRY),
        @ForeignBreakpoints.BreakpointDef(type = "org.neo4j.com.DechunkingChannelBuffer", method = "readNextChunk",
                on = Event.EXIT)})
@RunWith(SubProcessTestRunner.class)
@Ignore("This test depends on chuncked requests, otherwise it will hang. So either reduce the Protocol" +
        ".DEFAULT_FRAME_LENGTH to 1024"
        + "or create a huge difference in the stores between master and slave which will lead to a multichunk " +
        "response.")
public class TestClientThreadIsolation
{
    @Test
    @EnabledBreakpoints({"makeSureNextTransactionIsFullyFetched",
            "readNextChunk", "waitTxCopyToStart", "finish"})
    public void testTransactionsPulled() throws Exception
    {
        final HighlyAvailableGraphDatabase master =
                (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( TargetDirectory.forTest( TestClientThreadIsolation.class ).cleanDirectory(
                        "master" ).getAbsolutePath() ).
                setConfig( ClusterSettings.server_id, "1" ).
                newGraphDatabase();

        final HighlyAvailableGraphDatabase slave1 =
                (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( TargetDirectory.forTest( TestClientThreadIsolation.class ).cleanDirectory(
                        "slave1" ).getAbsolutePath() ).
                setConfig( ClusterSettings.cluster_server, "127.0.0.1:5002" ).
                setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" ).
                setConfig( ClusterSettings.server_id, "2" ).
                setConfig( HaSettings.max_concurrent_channels_per_slave, "2" ).
                setConfig( HaSettings.ha_server, "127.0.0.1:8001" ).
                newGraphDatabase();

        Transaction masterTx = master.beginTx();
        master.createNode().createRelationshipTo( master.createNode(),
                DynamicRelationshipType.withName( "master" ) ).setProperty(
                "largeArray", new int[20000] );
        masterTx.success();
        masterTx.finish();

        Thread thread1 = new Thread( new Runnable()
        {
            public void run()
            {
                // TODO Figure out how to do this
//                Master masterClient = slave1.getBroker().getMaster().first();
//                Response<Integer> response = masterClient.createRelationshipType(
//                        slave1.getSlaveContext( 10 ), "name" );
//                slave1.receive( response ); // will be suspended here
//                response.close();
            }
        }, "thread 1" );

        Thread thread2 = new Thread( new Runnable()
        {
            public void run()
            {
                /*
                 * We have two operations since we need to make sure this test passes
                 * before and after the proper channel releasing fix. The issue is
                 * that we can't have only one channel since it will deadlock because
                 * the txCopyingThread is suspended and won't release the channel
                 * (after the fix). But the problem is that with two channels going
                 * before the fix it won't break because the RR policy in
                 * ResourcePool will give the unused channel to the new requesting thread,
                 * thus not triggering the bug. The solution is to do two requests so
                 * eventually get the released, half consumed channel.
                 */
                // TODO Figure out how to do this
//                try
//                {
//                    waitTxCopyToStart();
//                    Master masterClient = slave1.getBroker().getMaster().first();
//                    SlaveContext ctx = slave1.getSlaveContext( 11 );
//                    Response<Integer> response = masterClient.createRelationshipType(
//                            ctx, "name2" );
//                    slave1.receive( response );
//                    response.close();
//
//                    // This will break before the fix
//                    response = masterClient.createRelationshipType(
//                            slave1.getSlaveContext( 12 ), "name3" );
//                    slave1.receive( response );
//                    response.close();
//
//                    /*
//                     * If the above fails, this won't happen. Used to fail the
//                     * test gracefully
//                     */
//                    Transaction masterTx = master.beginTx();
//                    master.getReferenceNode().createRelationshipTo(
//                            master.createNode(),
//                            DynamicRelationshipType.withName( "test" ) );
//                    masterTx.success();
//                    masterTx.finish();
//                }
//                finally
//                {
//                    finish();
//                }
            }
        }, "thread 2" );

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

//        assertTrue(
//                master.getReferenceNode().getRelationships(
//                        DynamicRelationshipType.withName( "test" ) ).iterator().hasNext() );
    }

    private static DebuggedThread txCopyingThread;
    private static DebuggedThread interferingThread;
    /*
     *  Blocks the txCopyingThread after reading the
     *  first chunk but before moving on to the next one.
     */
    private static CountDownLatch latch = new CountDownLatch( 1 );

    @BreakpointTrigger("waitTxCopyToStart")
    private void waitTxCopyToStart()
    {
        // wait for the first thread to grab the updates
    }

    @BreakpointTrigger("finish")
    private void finish()
    {
        // resume the suspended thread
    }

    @BreakpointHandler("waitTxCopyToStart")
    public static void onWaitTxCopyToStart( BreakPoint self, DebugInterface di )
    {
        interferingThread = di.thread().suspend( null );
        latch.countDown();
    }

    @BreakpointHandler("finish")
    public static void onFinish( BreakPoint self, DebugInterface di )
    {
        txCopyingThread.resume();
    }

    @BreakpointHandler("makeSureNextTransactionIsFullyFetched")
    public static void onStartingStoreCopy( BreakPoint self, DebugInterface di,
                                            @BreakpointHandler("readNextChunk") BreakPoint onReadNextChunk )
            throws Exception
    {
        // Wait for the other thread to recycle the channel
        latch.await();
        txCopyingThread = di.thread();
        self.disable();
    }

    @BreakpointHandler("readNextChunk")
    public static void onReadNextChunk( BreakPoint self, DebugInterface di )
            throws Exception
    {
        // Check because the interfering thread will trigger this too
        if ( txCopyingThread != null
                && di.thread().name().equals( txCopyingThread.name() ) )
        {
            txCopyingThread.suspend( null );
            interferingThread.resume();
            self.disable();
        }
    }
}
