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
package org.neo4j.kernel.impl.enterprise.store.id;


import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseDatabaseRule;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertThat;

public class RelationshipIdReuseStressIT
{

    @Rule
    public EmbeddedDatabaseRule embeddedDatabase = new EnterpriseDatabaseRule()
    {
        @Override
        protected void configure(GraphDatabaseBuilder builder )
        {
            super.configure( builder );
            builder.setConfig( EnterpriseEditionSettings.idTypesToReuse, IdType.RELATIONSHIP.name() );
        }
    };

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private String NAME_PROPERTY = "name";
    private static final int NUMBER_OF_BANDS = 3;
    private static final int NUMBER_OF_CITIES = 10;

    @After
    public void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    public void relationshipIdReused() throws Exception
    {
        Label cityLabel = DynamicLabel.label( "city" );
        final Label bandLabel = DynamicLabel.label( "band" );
        createBands( bandLabel );
        createCities( cityLabel );

        AtomicBoolean stopFlag = new AtomicBoolean( false );
        RelationshipsCreator relationshipsCreator = new RelationshipsCreator( stopFlag, bandLabel, cityLabel );

        List<Future> futures = new ArrayList<>();
        futures.add( startRelationshipRemoval( bandLabel, cityLabel, stopFlag ) );
        futures.add( startRelationshipsCreator( relationshipsCreator ) );
        futures.add( startRelationshipTypesCalculator( bandLabel, stopFlag ) );
        futures.add( startRelationshipCalculator( bandLabel, stopFlag ) );

        Thread.sleep( TimeUnit.SECONDS.toMillis( 5 ) );
        stopFlag.set( true );
        executorService.shutdown();
        executorService.awaitTermination( 5, TimeUnit.SECONDS );

        completeFutures( futures );

        long highestPossibleIdInUse = getHighestUsedIdForRelationships();
        assertThat( "Number of created relationships should be higher then highest possible id, since those are " +
                    "reused.", relationshipsCreator.getCreatedRelationships(),
                Matchers.greaterThan( highestPossibleIdInUse ) );
    }

    private long getHighestUsedIdForRelationships()
    {
        NeoStores neoStores = embeddedDatabase.getDependencyResolver().resolveDependency( NeoStores.class );
        return neoStores.getRelationshipStore().getHighestPossibleIdInUse();
    }

    private void completeFutures( List<Future> futures )
            throws InterruptedException, ExecutionException
    {
        for ( Future future : futures )
        {
            future.get();
        }
    }

    private void createCities( Label cityLabel )
    {
        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            for ( int i = 1; i <= NUMBER_OF_CITIES; i++ )
            {
                createLabeledNamedNode( cityLabel, "city" + i );
            }
            transaction.success();
        }
    }

    private void createBands( Label bandLabel )
    {
        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            for ( int i = 1; i <= NUMBER_OF_BANDS; i++ )
            {
                createLabeledNamedNode( bandLabel, "band" + i );
            }
            transaction.success();
        }
    }

    private Future<?> startRelationshipsCreator( RelationshipsCreator relationshipsCreator )
    {
        return executorService.submit( relationshipsCreator );
    }

    private Future<?> startRelationshipCalculator( final Label bandLabel, final AtomicBoolean stopFlag )
            throws ExecutionException, InterruptedException
    {
        return executorService.submit( new RelationshipCalculator( stopFlag, bandLabel ) );
    }

    private Future<?> startRelationshipTypesCalculator( final Label bandLabel, final AtomicBoolean stopFlag )
            throws ExecutionException, InterruptedException
    {
        return executorService.submit( new RelationshipTypeCalculator( stopFlag, bandLabel ) );
    }

    private Future<?> startRelationshipRemoval( final Label bandLabel, final Label cityLabel, AtomicBoolean stopFlag )
    {
        return executorService.submit( new RelationshipRemover( bandLabel, cityLabel, stopFlag ) );
    }

    private Direction getRandomDirection()
    {
        return Direction.values()[ThreadLocalRandom.current().nextInt( Direction.values().length )];
    }

    private TestRelationshipTypes getRandomRelationshipType()
    {
        return TestRelationshipTypes.values()[ThreadLocalRandom.current().nextInt( TestRelationshipTypes.values().length )];
    }

    private Node getRandomCityNode( EmbeddedDatabaseRule embeddedDatabase, Label cityLabel )
    {
        return embeddedDatabase.
                findNode( cityLabel, NAME_PROPERTY, "city" + (ThreadLocalRandom.current().nextInt( 1, NUMBER_OF_CITIES + 1 )) );
    }

    private Node getRandomBandNode( EmbeddedDatabaseRule embeddedDatabase, Label bandLabel )
    {
        return embeddedDatabase.
                findNode( bandLabel, NAME_PROPERTY, "band" + (ThreadLocalRandom.current().nextInt( 1, NUMBER_OF_BANDS + 1 )) );
    }

    private void createLabeledNamedNode( Label label, String name )
    {
        Node node = embeddedDatabase.createNode( label );
        node.setProperty( NAME_PROPERTY, name );
    }

    private enum TestRelationshipTypes implements RelationshipType
    {
        LIKE,
        HATE,
        NEUTRAL
    }

    private class RelationshipsCreator implements Runnable
    {
        private final AtomicBoolean stopFlag;
        private final Label bandLabel;
        private final Label cityLabel;

        private volatile long createdRelationships;

        RelationshipsCreator( AtomicBoolean stopFlag, Label bandLabel, Label cityLabel )
        {
            this.stopFlag = stopFlag;
            this.bandLabel = bandLabel;
            this.cityLabel = cityLabel;
        }

        @Override
        public void run()
        {
            while ( !stopFlag.get() )
            {
                int newRelationships = 0;
                try ( Transaction transaction = embeddedDatabase.beginTx() )
                {
                    Node bandNode = getRandomBandNode( embeddedDatabase, bandLabel );
                    int direction = ThreadLocalRandom.current().nextInt( 3 );
                    switch ( direction )
                    {
                        case 0:
                            newRelationships += connectCitiesToBand( bandNode );
                            break;
                        case 1:
                            newRelationships += connectBandToCities( bandNode );
                            break;
                        case 2:
                            newRelationships += connectCitiesToBand( bandNode );
                            newRelationships += connectBandToCities( bandNode );
                            break;
                        default:
                            throw new IllegalStateException("Unsupported direction value:" + direction);
                    }
                    transaction.success();
                }
                catch ( DeadlockDetectedException ignored )
                {
                    // deadlocks ignored
                }
                createdRelationships += newRelationships;
                long millisToWait = ThreadLocalRandom.current().nextLong( 10, 30 );
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( millisToWait ) );
            }
        }

        private int connectBandToCities( Node bandNode )
        {
            Node city1 = getRandomCityNode( embeddedDatabase, cityLabel );
            Node city2 = getRandomCityNode( embeddedDatabase, cityLabel );
            Node city3 = getRandomCityNode( embeddedDatabase, cityLabel );
            Node city4 = getRandomCityNode( embeddedDatabase, cityLabel );
            Node city5 = getRandomCityNode( embeddedDatabase, cityLabel );

            bandNode.createRelationshipTo( city1, TestRelationshipTypes.LIKE );
            bandNode.createRelationshipTo( city2, TestRelationshipTypes.LIKE );
            bandNode.createRelationshipTo( city3, TestRelationshipTypes.HATE );
            bandNode.createRelationshipTo( city4, TestRelationshipTypes.LIKE );
            bandNode.createRelationshipTo( city5, TestRelationshipTypes.NEUTRAL );
            return 5;
        }

        private int connectCitiesToBand( Node bandNode )
        {
            Node city1 = getRandomCityNode( embeddedDatabase, cityLabel );
            Node city2 = getRandomCityNode( embeddedDatabase, cityLabel );
            Node city3 = getRandomCityNode( embeddedDatabase, cityLabel );
            Node city4 = getRandomCityNode( embeddedDatabase, cityLabel );
            city1.createRelationshipTo( bandNode, TestRelationshipTypes.LIKE );
            city2.createRelationshipTo( bandNode, TestRelationshipTypes.HATE );
            city3.createRelationshipTo( bandNode, TestRelationshipTypes.LIKE );
            city4.createRelationshipTo( bandNode, TestRelationshipTypes.NEUTRAL );
            return 4;
        }

        long getCreatedRelationships()
        {
            return createdRelationships;
        }
    }

    private class RelationshipCalculator implements Runnable
    {
        private final AtomicBoolean stopFlag;
        private final Label bandLabel;
        private int relationshipSize;

        RelationshipCalculator( AtomicBoolean stopFlag, Label bandLabel )
        {
            this.stopFlag = stopFlag;
            this.bandLabel = bandLabel;
        }

        @Override
        public void run()
        {
            while ( !stopFlag.get() )
            {
                try ( Transaction transaction = embeddedDatabase.beginTx() )
                {
                    Node randomBandNode = getRandomBandNode( embeddedDatabase, bandLabel );
                    relationshipSize = IteratorUtil.asList( randomBandNode.getRelationships() ).size();
                    transaction.success();
                }
                long millisToWait = ThreadLocalRandom.current().nextLong( 10, 25 );
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( millisToWait ) );
            }
        }

        public int getRelationshipSize()
        {
            return relationshipSize;
        }
    }

    private class RelationshipTypeCalculator implements Runnable
    {
        private final AtomicBoolean stopFlag;
        private final Label bandLabel;
        private int relationshipSize;

        RelationshipTypeCalculator( AtomicBoolean stopFlag, Label bandLabel )
        {
            this.stopFlag = stopFlag;
            this.bandLabel = bandLabel;
        }

        @Override
        public void run()
        {
            while ( !stopFlag.get() )
            {
                try ( Transaction transaction = embeddedDatabase.beginTx() )
                {
                    Node randomBandNode = getRandomBandNode( embeddedDatabase, bandLabel );
                    relationshipSize = IteratorUtil.asList( randomBandNode.getRelationshipTypes()).size();
                    transaction.success();
                }
                long millisToWait = ThreadLocalRandom.current().nextLong( 10, 25 );
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( millisToWait ) );
            }
        }
    }

    private class RelationshipRemover implements Runnable
    {
        private final Label bandLabel;
        private final Label cityLabel;
        private AtomicBoolean stopFlag;

        RelationshipRemover( Label bandLabel, Label cityLabel, AtomicBoolean stopFlag )
        {
            this.bandLabel = bandLabel;
            this.cityLabel = cityLabel;
            this.stopFlag = stopFlag;
        }

        @Override
        public void run()
        {
            while ( !stopFlag.get() )
            {
                try ( Transaction transaction = embeddedDatabase.beginTx() )
                {
                    boolean deleteOnBands = ThreadLocalRandom.current().nextBoolean();
                    if ( deleteOnBands )
                    {
                        deleteRelationshipOfRandomType();
                    }
                    else
                    {
                        deleteRelationshipOnRandomNode();

                    }
                    transaction.success();
                }
                catch ( DeadlockDetectedException | NotFoundException ignored)
                {
                    // ignore deadlocks
                }
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 15 ) );
            }
        }

        private void deleteRelationshipOfRandomType()
        {
            Node bandNode = getRandomBandNode( embeddedDatabase, bandLabel );
            TestRelationshipTypes relationshipType = getRandomRelationshipType();
            Iterable<Relationship> relationships =
                    bandNode.getRelationships( relationshipType, getRandomDirection() );
            for ( Relationship relationship : relationships )
            {
                relationship.delete();
            }
        }

        private void deleteRelationshipOnRandomNode()
        {
            try ( ResourceIterator<Node> nodeResourceIterator = embeddedDatabase.findNodes( cityLabel ) )
            {
                List<Node> nodes = Iterables.toList( nodeResourceIterator );
                int index = ThreadLocalRandom.current().nextInt( nodes.size() );
                Node node = nodes.get( index );
                for ( Relationship relationship : node.getRelationships() )
                {
                    relationship.delete();
                }
            }
        }
    }
}
