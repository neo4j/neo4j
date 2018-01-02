/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.parboiled.common.StringUtils;

import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.helpers.Strings;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.Race;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.kernel.impl.api.KernelTransactions.tx_termination_aware_locks;

@RunWith( Parameterized.class )
public class TerminationOfSlavesDuringPullUpdatesIT
{
    private static final int READER_CONTESTANTS = 20;
    private static final int STRING_LENGTH = 20000;
    private static final int PROPERTY_KEY_CHAIN_LENGTH = 100;

    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( HaSettings.pull_interval, "0" )
            .withSharedSetting( HaSettings.tx_push_factor, "0" );

    @Parameter
    public ReadContestantActions action;
    @Parameter( 1 )
    public boolean txTerminationAwareLocks;
    @Parameter( 2 )
    public String name;

    @Parameters( name = "{1}" )
    public static Iterable<Object> data()
    {
        return Arrays.<Object>asList( new Object[][]
                {
                        {new PropertyValueActions( longString( 'a' ), longString( 'b' ), true ),
                                true, "NodeStringProperty[txTerminationAwareLocks=yes]"},
                        {new PropertyValueActions( longString( 'a' ), longString( 'b' ), true ),
                                false, "NodeStringProperty[txTerminationAwareLocks=no]"},

                        {new PropertyValueActions( longString( 'a' ), longString( 'b' ), false ),
                                true, "RelationshipStringProperty[txTerminationAwareLocks=yes]"},
                        {new PropertyValueActions( longString( 'a' ), longString( 'b' ), false ),
                                false, "RelationshipStringProperty[txTerminationAwareLocks=no]"},

                        {new PropertyValueActions( longArray( 'a' ), longArray( 'b' ), true ),
                                true, "NodeArrayProperty[txTerminationAwareLocks=yes]"},
                        {new PropertyValueActions( longArray( 'a' ), longArray( 'b' ), true ),
                                false, "NodeArrayProperty[txTerminationAwareLocks=no]"},

                        {new PropertyValueActions( longArray( 'a' ), longArray( 'b' ), false ),
                                true, "RelationshipArrayProperty[txTerminationAwareLocks=yes]"},
                        {new PropertyValueActions( longArray( 'a' ), longArray( 'b' ), false ),
                                false, "RelationshipArrayProperty[txTerminationAwareLocks=no]"},

                        {new PropertyKeyActions( 'a', 'b', true ),
                                true, "NodePropertyKeys[txTerminationAwareLocks=yes]"},
                        {new PropertyKeyActions( 'a', 'b', true ),
                                false, "NodePropertyKeys[txTerminationAwareLocks=no]"},

                        {new PropertyKeyActions( 'a', 'b', false ),
                                true, "RelationshipPropertyKeys[txTerminationAwareLocks=yes]"},
                        {new PropertyKeyActions( 'a', 'b', false ),
                                false, "RelationshipPropertyKeys[txTerminationAwareLocks=no]"}
                }
        );
    }

    @Test
    public void slavesTerminateOrReadConsistentDataWhenApplyingBatchLargerThanSafeZone() throws Throwable
    {
        long safeZone = TimeUnit.MILLISECONDS.toMillis( 0 );
        clusterRule.withSharedSetting( HaSettings.id_reuse_safe_zone_time, String.valueOf( safeZone ) );
        // given
        final ClusterManager.ManagedCluster cluster = startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        // when
        // ... slaves and master has node with long string property
        long entityId = action.createInitialEntity( master );
        cluster.sync();
        // ... and property is removed on master
        action.removeProperties( master, entityId );
        Thread.sleep( 100 );
        // ... and maintenance is called to make sure "safe" ids are freed to be reused
        forceMaintenance( master );
        // ... and a new property is created on master that
        action.setNewProperties( master, entityId );

        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        Race race = new Race();
        final AtomicBoolean end = new AtomicBoolean( false );
        for ( int i = 0; i < READER_CONTESTANTS; i++ )
        {
            race.addContestant( readContestant( action, entityId, slave, end ) );
        }

        race.addContestant( pullUpdatesContestant( slave, end ) );

        race.go();
    }

    @Test
    public void slavesDontTerminateAndReadConsistentDataWhenApplyingBatchSmallerThanSafeZone() throws Throwable
    {
        long safeZone = TimeUnit.MINUTES.toMillis( 1 );
        clusterRule.withSharedSetting( HaSettings.id_reuse_safe_zone_time, String.valueOf( safeZone ) );
        // given
        final ClusterManager.ManagedCluster cluster = startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        // when
        // ... slaves and master has node with long string property
        long entityId = action.createInitialEntity( master );
        cluster.sync();
        // ... and property is removed on master
        action.removeProperties( master, entityId );
        // ... and maintenance is called to make sure "safe" ids are freed to be reused
        forceMaintenance( master );
        // ... and a new property is created on master that
        action.setNewProperties( master, entityId );

        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        Race race = new Race();
        final AtomicBoolean end = new AtomicBoolean( false );
        for ( int i = 0; i < READER_CONTESTANTS; i++ )
        {
            race.addContestant( readContestant( action, entityId, slave, end ) );
        }

        race.addContestant( pullUpdatesContestant( slave, end ) );

        race.go();
    }

    private Runnable readContestant( final ReadContestantActions action, final long entityId,
            final HighlyAvailableGraphDatabase slave, final AtomicBoolean end )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                while ( !end.get() )
                {
                    try ( Transaction tx = slave.beginTx() )
                    {
                        for ( int i = 0; i < 10; i++ )
                        {
                            action.verifyProperties( slave, entityId );
                        }

                        tx.success();
                    }
                    catch ( TransactionTerminatedException | TransientTransactionFailureException ignored )
                    {
                    }
                }
            }
        };
    }

    private Runnable pullUpdatesContestant( final HighlyAvailableGraphDatabase slave, final AtomicBoolean end )
    {
        return new Runnable()
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
        };
    }

    private interface ReadContestantActions
    {
        long createInitialEntity( HighlyAvailableGraphDatabase db );

        void removeProperties( HighlyAvailableGraphDatabase db, long entityId );

        void setNewProperties( HighlyAvailableGraphDatabase db, long entityId );

        void verifyProperties( HighlyAvailableGraphDatabase db, long entityId );
    }

    private static class PropertyValueActions implements ReadContestantActions
    {
        static final String KEY = "key";
        final Object valueA;
        final Object valueB;
        final boolean node;

        PropertyValueActions( Object valueA, Object valueB, boolean node )
        {
            this.valueA = valueA;
            this.valueB = valueB;
            this.node = node;
        }

        @Override
        public long createInitialEntity( HighlyAvailableGraphDatabase db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                long id;
                if ( node )
                {
                    id = createInitialNode( db );
                }
                else
                {
                    id = createInitialRelationship( db );
                }
                tx.success();
                return id;
            }
        }

        long createInitialNode( HighlyAvailableGraphDatabase db )
        {
            Node node = db.createNode();
            node.setProperty( KEY, valueA );
            return node.getId();
        }

        long createInitialRelationship( HighlyAvailableGraphDatabase db )
        {
            Node start = db.createNode();
            Node end = db.createNode();
            Relationship relationship = start.createRelationshipTo( end, withName( "KNOWS" ) );
            relationship.setProperty( KEY, valueA );
            return relationship.getId();
        }

        @Override
        public void removeProperties( HighlyAvailableGraphDatabase db, long entityId )
        {
            try ( Transaction tx = db.beginTx() )
            {
                getEntity( db, entityId ).removeProperty( KEY );
                tx.success();
            }
        }

        @Override
        public void setNewProperties( HighlyAvailableGraphDatabase db, long entityId )
        {
            try ( Transaction tx = db.beginTx() )
            {
                getEntity( db, entityId ).setProperty( KEY, valueB );
                tx.success();
            }
        }

        @Override
        public void verifyProperties( HighlyAvailableGraphDatabase db, long entityId )
        {
            Object value = getEntity( db, entityId ).getProperty( KEY, null );
            assertPropertyValue( value, valueA, valueB );
        }

        PropertyContainer getEntity( HighlyAvailableGraphDatabase db, long id )
        {
            return node ? db.getNodeById( id ) : db.getRelationshipById( id );
        }
    }

    private static class PropertyKeyActions implements ReadContestantActions
    {
        final char keyPrefixA;
        final char keyPrefixB;
        final boolean node;

        PropertyKeyActions( char keyPrefixA, char keyPrefixB, boolean node )
        {
            this.keyPrefixA = keyPrefixA;
            this.keyPrefixB = keyPrefixB;
            this.node = node;
        }

        @Override
        public long createInitialEntity( HighlyAvailableGraphDatabase db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                long id;
                if ( node )
                {
                    id = createInitialNode( db );
                }
                else
                {
                    id = createInitialRelationship( db );
                }
                tx.success();
                return id;
            }
        }

        long createInitialNode( HighlyAvailableGraphDatabase db )
        {
            Node node = db.createNode();
            createPropertyChain( node, keyPrefixA );
            return node.getId();
        }

        long createInitialRelationship( HighlyAvailableGraphDatabase db )
        {
            Node start = db.createNode();
            Node end = db.createNode();
            Relationship relationship = start.createRelationshipTo( end, withName( "KNOWS" ) );
            createPropertyChain( relationship, keyPrefixA );
            return relationship.getId();
        }

        void createPropertyChain( PropertyContainer entity, char prefix )
        {
            for ( int i = 0; i < PROPERTY_KEY_CHAIN_LENGTH; i++ )
            {
                entity.setProperty( "" + prefix + i, i );
            }
        }

        @Override
        public void removeProperties( HighlyAvailableGraphDatabase db, long entityId )
        {
            try ( Transaction tx = db.beginTx() )
            {
                PropertyContainer entity = getEntity( db, entityId );
                for ( String key : entity.getPropertyKeys() )
                {
                    entity.removeProperty( key );
                }
                tx.success();
            }
        }

        @Override
        public void setNewProperties( HighlyAvailableGraphDatabase db, long entityId )
        {
            try ( Transaction tx = db.beginTx() )
            {
                createPropertyChain( getEntity( db, entityId ), keyPrefixB );
                tx.success();
            }
        }

        @Override
        public void verifyProperties( HighlyAvailableGraphDatabase db, long entityId )
        {
            assertPropertyChain( getEntity( db, entityId ).getAllProperties().keySet(), keyPrefixA, keyPrefixB );
        }

        PropertyContainer getEntity( HighlyAvailableGraphDatabase db, long id )
        {
            return node ? db.getNodeById( id ) : db.getRelationshipById( id );
        }
    }

    private ClusterManager.ManagedCluster startCluster() throws Exception
    {
        return clusterRule.withSharedSetting( tx_termination_aware_locks, String.valueOf( txTerminationAwareLocks ) )
                .startCluster();
    }

    private static Object longArray( char b )
    {
        return longString( b ).toCharArray();
    }

    private static String longString( char ch )
    {
        return StringUtils.repeat( ch, STRING_LENGTH );
    }

    private void forceMaintenance( HighlyAvailableGraphDatabase master )
    {
        master.getDependencyResolver()
                .resolveDependency( NeoStoreDataSource.BufferedIdMaintenanceController.class )
                .maintenance();
    }

    private static void assertPropertyValue( Object property, Object... candidates )
    {
        if ( property == null )
        {
            return;
        }
        for ( Object candidate : candidates )
        {
            if ( Objects.deepEquals( property, candidate ) )
            {
                return;
            }
        }
        fail( "property value was " + Strings.prettyPrint( property ) );
    }

    private static void assertPropertyChain( Set<String> allProperties, Character... keyPrefix )
    {
        boolean first = true;
        Character actualFirst = null;
        for ( String key : allProperties )
        {
            if ( first )
            {
                first = false;
                actualFirst = key.charAt( 0 );
                assertThat( "Other prefix than expected", actualFirst, isIn( keyPrefix ) );
            }
            assertThat( "Property key chain is broken " + Arrays.toString( allProperties.toArray() ),
                    key.charAt( 0 ), equalTo( actualFirst ) );
        }
    }
}
