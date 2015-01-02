/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcessTestRunner;

/*
 * This test fails. What it tries to assert is that when a slave pulls updates from the master they are applied
 * atomically. Currently this does not happen and the test case breaks. So it must remain ignored either until we
 * have 2PC semantics on updates OR neostore and lucene become the same datasource.
 *
 * More detailed. The master creates a node and indexes it. The slave copies the store. The master deletes the node
 * from the index and from the store. The slave pulls updates but crashes between the application of the two
 * datasources. Since lucene-index is applied first, the slave will observe the node missing from the index but
 * existing in the neostore. If this was an autoindex, it would appear that the node exists but is not indexed.
 */
@ForeignBreakpoints({
        @ForeignBreakpoints.BreakpointDef(type = "org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager",
                method = "applyCommittedTransaction", on = BreakPoint.Event.ENTRY)})
@RunWith(SubProcessTestRunner.class)
@Ignore("Never passed")
public class TestPartialPullUpdates
{
    private HighlyAvailableGraphDatabase master;
    private HighlyAvailableGraphDatabase slave1;

    @Before
    public void setup() throws Exception
    {
        master = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( TargetDirectory.forTest( TestPartialPullUpdates.class ).cleanDirectory(
                        "master" ).getAbsolutePath() ).
                setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" ).
                setConfig( ClusterSettings.server_id, "1" ).
                setConfig( HaSettings.tx_push_factor, "0" ).
                newGraphDatabase();
        Transaction tx = master.beginTx();
        Node node = master.createNode();
        node.setProperty( "uuid", "123" );
        master.index().forNodes( "auto" ).add( node, "uuid", "123" );
        tx.success();
        tx.finish();

        slave1 = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( TargetDirectory.forTest( TestPartialPullUpdates.class ).cleanDirectory(
                        "slave1" ).getAbsolutePath() ).
                setConfig( ClusterSettings.cluster_server, "127.0.0.1:5002" ).
                setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" ).
                setConfig( ClusterSettings.server_id, "2" ).
                setConfig( HaSettings.ha_server, "localhost:6362" ).
                setConfig( HaSettings.pull_interval, "0" ).
                newGraphDatabase();
    }

    @After
    public void shutdown()
    {
        slave1.shutdown();
        master.shutdown();
    }

    @Test
    @EnabledBreakpoints({"applyCommittedTransaction"})
    public void doTheDamnTest() throws Exception
    {
        // Ensure the slave has got the store - simple sanity check
        assertEquals( "123", slave1.getNodeById( 1 ).getProperty( "uuid" ) );
        // Delete the node from the store and the index at the slave. No push factor.
        Transaction tx = master.beginTx();
        Node toRemove = master.index().forNodes( "auto" ).get( "uuid", "123" ).next();
        long nodeId = toRemove.getId();
        master.index().forNodes( "auto" ).remove( toRemove );
        toRemove.delete();
        tx.success();
        tx.finish();

        // Do the update pulling in a different thread so we can kill it.
        Thread t = new Thread( new Runnable()
        {
            public void run()
            {
                slave1.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            }
        } );
        t.start();
        t.join();

        // It should either be in both the graph and the index or in neither.
        if ( slave1.index().forNodes( "auto" ).get( "uuid", "123" ).hasNext() )
        {
            System.out.println( "it was in the index" );
            assertEquals( "123", slave1.getNodeById( nodeId ).getProperty( "uuid" ) );
        }
        else
        {
            System.out.println( "it was not in the index" );
            try
            {
                slave1.getNodeById( nodeId );
                fail( "Node should not be there" );
            }
            catch ( NotFoundException e )
            {
                // good
            }
        }
    }

    /*
     * The applyCommittedTransaction method is called from ServerUtil.applyReceivedTransactions(). In the test method
     * it is called twice, both from a single instance of the apply loop. We let the first one pass but we kill the
     * second. Since it is in a loop, we cannot build a stack that shows when to kill it. The counter solution is
     * relatively safe and very much simple.
     */
    @BreakpointHandler("applyCommittedTransaction")
    public static void onApplyCommittedHandler( BreakPoint self, DebugInterface di )
    {
        if ( self.invocationCount() < 2 )
        {
            di.thread().resume();
        }
        else
        {
            di.thread().stop();
        }
    }
}
