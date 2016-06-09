/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

import org.junit.Rule;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.test.Race;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.ha.factory.HighlyAvailableEditionModule.id_reuse_safe_zone_time;

public class SlavesCanReadCorruptedData
{
    // TODO: Remove this test when implementation is done and verified
    @Rule
    public RepeatRule repeatRule = new RepeatRule();
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( HaSettings.pull_interval, "0" )
            .withSharedSetting( HaSettings.tx_push_factor, "0" );

    @RepeatRule.Repeat( times = 10 )
    @Test
    public void slavesTerminateOrReadConsistentDataWhenApplyingBatchLargerThanSafeZone() throws Throwable
    {
        long safeZone = TimeUnit.MILLISECONDS.toMillis( 0 );
        clusterRule.withSharedSetting( id_reuse_safe_zone_time, String.valueOf( safeZone ) );
        // given
        final ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        int stringLength = 1000;

        final String stringA = buildString( stringLength, 'a' );
        final String stringB = buildString( stringLength, 'b' );

        final String key = "key";

        // when
        // ... slaves and master has node with long string property
        final long nodeId = createNodeAndSetProperty( master, key, stringA );
        cluster.sync();
        // ... and property is removed on master
        removeProperty( master, nodeId, key );
        Thread.sleep( 100 );
        // ... and maintenance is called to make sure "safe" ids are freed to be reused
        forceMaintenance( master );
        // ... and a new property is created on master that
        setProperty( master, nodeId, key, stringB );

        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        Race race = new Race();
        int nbrOfReaders = 100;
        final AtomicBoolean end = new AtomicBoolean( false );
        final AtomicInteger successfulReads = new AtomicInteger();
        final AtomicInteger transientExceptions = new AtomicInteger();
        for ( int i = 0; i < nbrOfReaders; i++ )
        {
            race.addContestant(
                    readContestant( stringA, stringB, key, nodeId, slave, end, successfulReads, transientExceptions ) );
        }

        race.addContestant( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Random rnd = new Random();
                    Thread.sleep( rnd.nextInt( 100 ) );
                    slave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    end.set( true );
                }
            }
        } );

        race.go();
        assertTrue( "Expected at least one transaction to be interrupted when applying update",
                transientExceptions.get() > 0 );
    }

    @RepeatRule.Repeat( times = 10 )
    @Test
    public void slavesDontTerminateAndReadConsistentDataWhenApplyingBatchSmallerThanSafeZone() throws Throwable
    {
        long safeZone = TimeUnit.MINUTES.toMillis( 1 );
        clusterRule.withSharedSetting( id_reuse_safe_zone_time, String.valueOf( safeZone ) );
        // given
        final ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        int stringLength = 1000;

        final String stringA = buildString( stringLength, 'a' );
        final String stringB = buildString( stringLength, 'b' );

        final String key = "key";

        // when
        // ... slaves and master has node with long string property
        final long nodeId = createNodeAndSetProperty( master, key, stringA );
        cluster.sync();
        // ... and property is removed on master
        removeProperty( master, nodeId, key );
        // ... and maintenance is called to make sure "safe" ids are freed to be reused
        forceMaintenance( master );
        // ... and a new property is created on master that
        setProperty( master, nodeId, key, stringB );

        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        Race race = new Race();
        int nbrOfReaders = 100;
        final AtomicBoolean end = new AtomicBoolean( false );
        final AtomicInteger successfulReads = new AtomicInteger();
        final AtomicInteger transientExceptions = new AtomicInteger();
        for ( int i = 0; i < nbrOfReaders; i++ )
        {
            race.addContestant(
                    readContestant( stringA, stringB, key, nodeId, slave, end, successfulReads, transientExceptions ) );
        }

        race.addContestant( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    slave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    end.set( true );
                }
            }
        } );

        race.go();
        assertEquals( "Did not expect any transactions to be terminated", transientExceptions.get(), 0 );
    }

    Runnable readContestant( final String stringA, final String stringB, final String key, final long nodeId,
            final HighlyAvailableGraphDatabase slave, final AtomicBoolean end, final AtomicInteger successfulReads,
            final AtomicInteger transientExceptions )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                int reads = 0;
                int fails = 0;
                try
                {
                    while ( !end.get() )
                    {
                        String property;
                        try ( Transaction tx = slave.beginTx() )
                        {
                            Node node = slave.getNodeById( nodeId );
                            for ( int i = 0; i < 10; i++ )
                            {
                                property = (String) node.getProperty( key, null );
                                assertPropertyValue( property, stringA, stringB );
                            }

                            tx.success();
                        }
                        catch ( TransientFailureException e )
                        {
                            // OK
                            fails++;
                            continue;
                        }
                        reads++;
                    }
                }
                finally
                {
                    successfulReads.addAndGet( reads );
                    transientExceptions.addAndGet( fails );
                }
            }
        };
    }
//
//    @Test
//    public void slavesNowWorkAsTheyShould() throws Throwable
//    {
//        /**
//         * master
//         *                    |------|
//         * |------------------------>
//         *
//         * upToDateSlave
//         * |-------------------->|<->
//         *
//         * laggingSlave
//         * |------>|<--------------->
//         *
//         */
//
//        // given
//        final ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
//        final HighlyAvailableGraphDatabase master = cluster.getMaster();
//        int stringLength = 1000;
//
//        final String[] values = new String[]{
//                buildString( stringLength, 'a' ),
//                buildString( stringLength, 'b' ),
//                buildString( stringLength, 'c' )
//        };
//        final String key = "key";
//
//
//        // when
//        final AtomicBoolean end = new AtomicBoolean( false );
//        // ... slaves and master has node with long string property
//        final long nodeId = createNodeAndSetProperty( master, key, values[0] );
//        cluster.sync();
//
//        Race race = new Race();
//
//        // updater on master
//        final long writeInterval = ID_REUSE_QUARANTINE_TIME / 4;
//        race.addContestant( new Runnable()
//        {
//            int value = 0;
//            int count = 0;
//            @Override
//            public void run()
//            {
//                while ( count++ < 20 )
//                {
//                    try
//                    {
//                        forceMaintenance( master );
//                        setProperty( master, nodeId, key, values[value] );
//                        value = (value + 1) % values.length;
//                        Thread.sleep( writeInterval );
//                    }
//                    catch ( InterruptedException e )
//                    {
//                        e.printStackTrace();
//                    }
//                }
//                end.set( true );
//            }
//        } );
//
//        // SLAVES
//        HighlyAvailableGraphDatabase upToDateSlave = cluster.getAnySlave();
//        HighlyAvailableGraphDatabase laggingSlave = cluster.getAnySlave( upToDateSlave );
//        int nbrOfReaders = 100;
//
//        // UP TO DATE
//        long updateInterval = ID_REUSE_QUARANTINE_TIME / 2;
//        for ( int i = 0; i < nbrOfReaders; i++ )
//        {
//            race.addContestant( propertyReader( values, key, end, nodeId, upToDateSlave, "upToDateSlave" ) );
//        }
//        race.addContestant( updatePuller( end, upToDateSlave, updateInterval, "upToDateSlave" ) );
//
//        // LAGGING
//        long slowUpdateInterval = ID_REUSE_QUARANTINE_TIME * 5;
//        for ( int i = 0; i < nbrOfReaders; i++ )
//        {
//            race.addContestant( propertyReader( values, key, end, nodeId, laggingSlave, "laggingSlave" ) );
//        }
//        race.addContestant( updatePuller( end, laggingSlave, slowUpdateInterval, "laggingSlave" ) );
//
//        race.go();
//        cluster.shutdown();
//    }
//
//    Runnable updatePuller( final AtomicBoolean end, final HighlyAvailableGraphDatabase slave,
//            final long updateInterval, String slaveName )
//    {
//        return new Runnable()
//        {
//            @Override
//            public void run()
//            {
//                while ( !end.get() )
//                {
//                    try
//                    {
//                        Thread.sleep( updateInterval );
//                        slave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
//                    }
//                    catch ( InterruptedException e )
//                    {
//                        throw new RuntimeException( e );
//                    }
//                }
//            }
//        };
//    }
//
//    Runnable propertyReader( final String[] values, final String key, final AtomicBoolean end, final long nodeId,
//            final HighlyAvailableGraphDatabase slave, final String slaveName )
//    {
//        return new Runnable()
//        {
//            @Override
//            public void run()
//            {
//                while ( !end.get() )
//                {
//                    Object property = null;
//                    boolean boom = false;
//                    boolean txFailure = false;
//                    try ( Transaction tx = slave.beginTx() )
//                    {
//                        Node node = slave.getNodeById( nodeId );
//                        property = node.getProperty( key );
//                        tx.success();
//                    }
//                    catch ( Exception e )
//                    {
//                        if ( e instanceof TransactionTerminatedException ||
//                             e.getCause() instanceof TransactionTerminatedException )
//                        {
//                            System.out.println( "[" + slaveName + "] Transaction terminated" );
//                            boom = true;
//                            txFailure = true;
//                        }
//                        else
//                        {
//                            e.printStackTrace();
//                            boom = true;
//                            txFailure = true;
//                        }
//                    }
//                    if ( !txFailure )
//                    {
//                        try
//                        {
//                            assertPropertyValue( property, values );
//                        }
//                        catch ( AssertionError e )
//                        {
//                            System.out.println( "[" + slaveName + "] Assertion error" );
//                            boom = true;
//                        }
//                    }
//
//                    if ( boom )
//                    {
//                        try
//                        {
//                            Thread.sleep( 1000 );
//                        }
//                        catch ( InterruptedException e )
//                        {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }
//        };
//    }

    String buildString( int stringLength, char c )
    {
        StringBuilder sba = new StringBuilder();
        for ( int i = 0; i < stringLength; i++ )
        {
            sba.append( c );
        }
        return sba.toString();
    }

    private long createNodeAndSetProperty( HighlyAvailableGraphDatabase master, String propertyKey,
            String propertyValue )
    {
        long ida;
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode();
            ida = node.getId();
            node.setProperty( propertyKey, propertyValue );

            tx.success();
        }
        return ida;
    }

    private long createNode( HighlyAvailableGraphDatabase master )
    {
        long ida;
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode();
            ida = node.getId();

            tx.success();
        }
        return ida;
    }

    private void setProperty( HighlyAvailableGraphDatabase master, long nodeId, String propertyKey,
            String propertyValue )
    {
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.getNodeById( nodeId );
            node.setProperty( propertyKey, propertyValue );

            tx.success();
        }
    }

    private void forceMaintenance( HighlyAvailableGraphDatabase master )
    {
        NeoStoreDataSource dataSource =
                master.getDependencyResolver().resolveDependency( DataSourceManager.class ).getDataSource();
        dataSource.maintenance();
    }

    private void removeProperty( HighlyAvailableGraphDatabase master, long nodeId, String propertyKey )
    {
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.getNodeById( nodeId );
            node.removeProperty( propertyKey );

            tx.success();
        }
    }

    private void assertPropertyValue( Object property, String... candidates )
    {
        if ( property == null )
        {
            return;
        }
        for ( String candidate : candidates )
        {
            if ( property.equals( candidate ) )
            {
                return;
            }
        }
        fail( "property value was " + property );
    }
}
