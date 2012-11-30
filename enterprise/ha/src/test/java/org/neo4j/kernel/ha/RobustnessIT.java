/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.consistency.checking.incremental.intercept.VerifyingTransactionInterceptorProvider;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.test.TargetDirectory;

public class RobustnessIT
{
    private final File path = TargetDirectory.forTest( getClass() ).graphDbDir( true );
    private HighlyAvailableGraphDatabase[] dbs;

    @Test
    public void bringUpClusterAndIssueSomeWriteCommandsOnEachMember() throws Exception
    {
        dbs = new HighlyAvailableGraphDatabase[3];
        dbs[0] = startDb( 0 );
        final List<Node> nodes = Collections.synchronizedList( new LinkedList() );
        final List<Relationship> rels = Collections.synchronizedList( new LinkedList<Relationship>() );
        for ( int i = 0; i < 10; i++ )
        {
            createInitial( dbs[0], nodes, rels );
        }

        dbs[1] = startDb( 1 );
        assertExists( dbs[1], nodes, rels );
        dbs[2] = startDb( 2 );
        assertExists( dbs[2], nodes, rels );
/*
        // Case 1: Cluster is running, do stuff on each instance, see they are there
        System.out.println( "============== Case simple create on all ================" );
        ExecutorService threadPool = Executors.newFixedThreadPool( 30 );
        for ( int i = 0; i < 1000; i++ )
        {
            for ( final HighlyAvailableGraphDatabase db : dbs )
            {
                threadPool.execute( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for ( int i = 0; i < 10; i++ )
                        {
                            createNodeAndRelationship( db, nodes, rels );
                        }
                    }
                } );
            }
        }

        threadPool.shutdown();
        while ( !threadPool.awaitTermination( 10, TimeUnit.SECONDS ) )
        {
            ;
        }

//        for ( HighlyAvailableGraphDatabase db : dbs )
//        {
//            if ( !db.isMaster() )
//            {
//                db.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
//            }
//            assertExists( db, nodes, rels );
//        }

//        for ( int i = dbs.length - 1; i >= 0; i-- )
        for ( int i = 0; i < dbs.length; i++ )
        {
            HighlyAvailableGraphDatabase db = dbs[i];
            if ( !db.isMaster() )
            {
                db.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            }
            db.shutdown();
            Thread.sleep( 3000 );
            ConsistencyCheckTool.main( new String[]{db.getStoreDir(), "-recovery"} );
        }
*/
//        System.exit( 0 );

        System.out.println( "============== Case simple master switch test ================" );

        System.out.println( "Start master test" );
        dbs[0].shutdown();
        System.out.println( "0 is now dead" );
        Thread.sleep( 3000 );
        assertTrue( dbs[1].isMaster() || dbs[2].isMaster() );
        dbs[0] = startDb( 0 );
        System.out.println( "0 is now back on" );
        assertTrue( dbs[1].isMaster() );
        assertFalse( dbs[0].isMaster() );

        System.out.println( "============== Case brutal master switch test with create ================" );
        for ( int i = 0; i < 6; i++ )
        {
            int j = findMaster();
            HighlyAvailableGraphDatabase db1 = dbs[(j + 1) % dbs.length];
            HighlyAvailableGraphDatabase db2 = dbs[(j + 2) % dbs.length];
            db1.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            db2.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            System.out.println( "Starting kill of " + j );
            dbs[j].shutdown();
            System.out.println( "========> killed " + j );
            for ( int k = 0; k < 10; k++ )
            {
//                    System.out.println("Creating on "+((j+1)%dbs.length));
                createNodeAndRelationship( db1, nodes, rels );
//                    System.out.println("Creating on "+((j+2)%dbs.length));
                createNodeAndRelationship( db2, nodes, rels );
            }
            Thread.sleep( 3000 );
            System.out.println( "Starting " + dbs[j] );
            dbs[j] = startDb( j );
            System.out.println( "Done starting " + j );
            Thread.sleep( 3000 );
            for ( int k = 0; k < 10; k++ )
            {
                    System.out.println("Creating on "+((j+1)%dbs.length));
                createNodeAndRelationship( db1, nodes, rels );
                    System.out.println("Creating on "+((j+2)%dbs.length));
                createNodeAndRelationship( db2, nodes, rels );
                    System.out.println("Creating on "+((j+3)%dbs.length));
                createNodeAndRelationship( dbs[(j + 3) % dbs.length], nodes, rels );
            }
        }
        int currentMaster = findMaster();

        HighlyAvailableGraphDatabase master = dbs[currentMaster];
        assertExists( master, nodes, rels );

        HighlyAvailableGraphDatabase db1 = dbs[(currentMaster + 1) % dbs.length];
        db1.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        assertExists( db1, nodes, rels );

        HighlyAvailableGraphDatabase db2 = dbs[(currentMaster + 2) % dbs.length];
        db2.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        assertExists( db2, nodes, rels );

        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            db.shutdown();
            Thread.sleep( 1000 );
            ConsistencyCheckTool.main( new String[]{db.getStoreDir(), "-recovery"} );
        }

//        System.exit( 0 );

        dbs = startCluster( 3 );

        // Case 2: Kill master, create stuff on new slave, see it is there
        System.out.println( "============== Case switch master, create on slave simple ================" );
        dbs[0].shutdown();
        for ( int i = 0; i < 1; i++ )
        {
            for ( int db = 1; db < dbs.length; db++ )
            {
                createNodeAndRelationship( dbs[db], nodes, rels );
            }
        }
        dbs[2].getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
//        dbs[1].shutdown();
//        dbs[2].shutdown();
        assertExists( dbs[1], nodes, rels );
        assertExists( dbs[2], nodes, rels );

//        ConsistencyCheck.run( dbs[0].getStoreDir(), dbs[0].getConfig() );
//        ConsistencyCheck.run( dbs[1].getStoreDir(), dbs[1].getConfig() );
//        ConsistencyCheck.run( dbs[2].getStoreDir(), dbs[2].getConfig() );
//        System.exit( 0 );

        // Case 3: Kill new master, leave one machine in cluster. See it works.
        System.out.println( "============== Case remove master, slave alone still works ================" );
        dbs[1].shutdown();
        createNodeAndRelationship( dbs[2], nodes, rels );
        assertExists( dbs[2], nodes, rels );

//        ConsistencyCheck.run( dbs[0].getStoreDir(), dbs[0].getConfig() );

        // Case 4: Start new instance, see it joins the cluster and works
        System.out.println( "============== Case instance joins single machine cluster ================" );
        dbs[1] = startDb( 1 );
        createNodeAndRelationship( dbs[1], nodes, rels );
        assertExists( dbs[1], nodes, rels );
        assertExists( dbs[2], nodes, rels );

//        ConsistencyCheck.run( dbs[0].getStoreDir(), dbs[0].getConfig() );

        // Case 5: Remove slave, see master is still up
        System.out.println( "============== Case remove slave, master still working ================" );
        dbs[1].shutdown();
        while ( true )
        {
            try
            {
                createNodeAndRelationship( dbs[2], nodes, rels );
                break;
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                continue;
            }
        }
        assertExists( dbs[2], nodes, rels );

        // Done
        System.out.println( "============== Done ================" );
        dbs[2].shutdown();

        for ( HighlyAvailableGraphDatabase db : dbs )
        {
//            db.shutdown();
            Thread.sleep( 3000 );
            System.out.println( "Checking " + db );
//            ConsistencyCheck.run( db.getStoreDir(), db.getConfig() );
        }
    }

    private void createInitial( HighlyAvailableGraphDatabase db, List<Node> nodes, List<Relationship> relationships )
    {
        while ( true )
        {
            Transaction tx = db.beginTx();
            try
            {
                Node node = db.createNode();
                nodes.add( node );
                Relationship rel = db.getReferenceNode().createRelationshipTo( node, MyRelTypes.TEST );
                relationships.add( rel );
                tx.success();
                return;
            }
            catch ( Exception e )
            {
                try
                {
                    Thread.sleep( 500 );
                }
                catch ( InterruptedException e1 )
                {
                    throw new RuntimeException( e1 );
                }
                e.printStackTrace();
                continue;
            }
            finally
            {
                try
                {
                    tx.finish();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        }
    }

    private int findMaster()
    {
        for ( int i = 0; i < dbs.length; i++ )
        {
            if ( dbs[i].isMaster() )
            {
                System.out.println( "Found master as " + i );
                return i;
            }
        }
        return -1;
    }

    private void assertExists( HighlyAvailableGraphDatabase db, List<Node> nodes, List<Relationship> rels )
    {
        for ( Node n : nodes )
        {
            db.getNodeById( n.getId() );
        }
        for ( Relationship rel : rels )
        {
            Relationship thisRel = db.getRelationshipById( rel.getId() );
            assertEquals( db.getRelationshipById( rel.getId() ).getStartNode(), db.getRelationshipById( thisRel.getId()).getStartNode() );
            assertEquals( db.getRelationshipById( rel.getId()).getEndNode(), db.getRelationshipById( thisRel.getId()).getEndNode() );
        }
        int nodeCount = 0;
        int relCount = 0;
        for ( Node n : db.getAllNodes() )
        {
            nodeCount++;
            for ( Relationship rel : n.getRelationships( Direction.OUTGOING ) )
            {
                relCount++;
            }
        }
        assertEquals( nodes.size() + 1/*ref node*/, nodeCount );
        assertEquals( rels.size(), relCount );
    }

    private Relationship findRelationship( Node referenceNode, Relationship other )
    {
        for ( Relationship rel : referenceNode.getRelationships() )
        {
            if ( rel.equals( other ) )
            {
                return rel;
            }
        }
        fail( other + " not found in " + referenceNode.getGraphDatabase() );
        return null;
    }

    private void createNodeAndRelationship( HighlyAvailableGraphDatabase db, List<Node> nodes, List<Relationship> rels )
    {
        while ( true )
        {
            Transaction tx = db.beginTx();
            try
            {
                // Create circumference
                Node node = db.createNode();
                nodes.add( node );
                Node from = db.getNodeById( nodes.get( nodes.size() - 1 ).getId() );
                Relationship rel = from.createRelationshipTo( node, MyRelTypes.TEST );
                rels.add( rel );
                // Create radius
                Relationship rad = db.getReferenceNode().createRelationshipTo( node, MyRelTypes.TEST );
                rels.add( rad );
                tx.success();
                return;
            }
            catch ( Exception e )
            {
                try
                {
                    Thread.sleep( 500 );
                }
                catch ( InterruptedException e1 )
                {
                    throw new RuntimeException( e1 );
                }
                e.printStackTrace();
//                tx.failure();
                continue;
            }
            finally
            {
                try
                {
                    tx.finish();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        }
    }

    private HighlyAvailableGraphDatabase[] startCluster( int size )
    {
        HighlyAvailableGraphDatabase[] dbs = new HighlyAvailableGraphDatabase[size];

        for ( int serverId = 0; serverId < size; serverId++ )
        {
            dbs[serverId] = startDb( serverId );
        }
        return dbs;
    }

    private HighlyAvailableGraphDatabase startDb( int serverId )
    {
        GraphDatabaseBuilder builder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( path( serverId ) )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003" )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + serverId) )
                .setConfig( HaSettings.server_id, "" + serverId )
                .setConfig( HaSettings.ha_server, ":" + (8001 + serverId) )
                .setConfig( HaSettings.tx_push_factor, "0" )
                .setConfig( GraphDatabaseSettings.intercept_committing_transactions, "true" )
                .setConfig( GraphDatabaseSettings.intercept_deserialized_transactions, "true" )
                .setConfig( TransactionInterceptorProvider.class.getSimpleName() + "." +
                        VerifyingTransactionInterceptorProvider.NAME, "true" );
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) builder.newGraphDatabase();
        Transaction tx = db.beginTx();
        tx.finish();
        try
        {
            Thread.sleep( 2000 );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        return db;
    }

    private String path( int i )
    {
        return new File( path, "" + i ).getAbsolutePath();
    }
}
