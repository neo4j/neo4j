/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.stresstests;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.concurrent.Futures;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helper.RepeatUntilCallable;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.IdContainer;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;
import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;
import static org.neo4j.function.Suppliers.untilTimeExpired;
import static org.neo4j.helper.StressTestingHelper.fromEnv;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;

public class IdReusabilityStressTesting
{
    private static final String DEFAULT_NUMBER_OF_CORES = "3";
    private static final String DEFAULT_DURATION_IN_MINUTES = "30";
    private static final String DEFAULT_REELECT_INTERVAL_IN_SECONDS = "60";
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();
    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName( "testType" );

    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    private final DefaultFileSystemRule defaultFileSystemRule = new DefaultFileSystemRule();
    private DefaultFileSystemAbstraction fs;

    @Before
    public void setUp() throws Exception
    {
        fs = defaultFileSystemRule.get();
    }

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        int numberOfCores = parseInt( fromEnv( "ID_REUSE_STRESS_NUMBER_OF_CORES", DEFAULT_NUMBER_OF_CORES ) );
        long durationInMinutes = parseLong( fromEnv( "ID_REUSE_STRESS_DURATION_IN_MINUTES", DEFAULT_DURATION_IN_MINUTES ) );
        long reelectInterval = parseLong( fromEnv( "ID_REUSE_STRESS_REELECT_INTERVAL_IN_SECONDS", DEFAULT_REELECT_INTERVAL_IN_SECONDS) );
        String workingDirectory = fromEnv( "ID_REUSE_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );

        File clusterDirectory = new File( workingDirectory, "cluster" );
        FileUtils.deleteRecursively( clusterDirectory ); // Make sure it is empty

        // Configure cluster
        clusterRule.withNumberOfCoreMembers( numberOfCores )
                .withRecordFormat( Standard.LATEST_NAME )
                .withClusterDirectory( clusterDirectory );

        // Startup cluster
        Cluster cluster = clusterRule.startCluster();

        // Save store directories
        List<String> storeDirectories = cluster.coreMembers().stream()
                .map( member -> member.storeDir().getAbsolutePath() )
                .collect( Collectors.toList() );

        // Create initial data
        System.out.println( "Created a million nodes for initial state..." );
        createInitialData( cluster );
        System.out.println( "...done" );

        // Setup worker context
        AtomicBoolean stopTheWorld = new AtomicBoolean();
        BooleanSupplier notExpired = untilTimeExpired( durationInMinutes, MINUTES );
        BooleanSupplier keepGoing = () -> !stopTheWorld.get() && notExpired.getAsBoolean();
        Runnable onFailure = () -> stopTheWorld.set( true );
        ExecutorService service = Executors.newCachedThreadPool();

        // Run stress test
        try
        {
            System.out.println( "Starting stress testing" );
            Future<?> insertLoad = service.submit( new InsertionWorkload( keepGoing, onFailure, cluster ) );
            Future<?> deleteLoad1 = service.submit( new DeletionWorkload( keepGoing, onFailure, cluster, 2_000_000 ) );
            Future<?> deleteLoad2 = service.submit( new DeletionWorkload( keepGoing, onFailure, cluster, 2_000_000 ) );
            Future<?> reelectionLoad = service.submit( new ReelectionWorkload( keepGoing, onFailure, cluster, reelectInterval ) );

            Futures.combine( insertLoad, deleteLoad1, deleteLoad2, reelectionLoad ).get( durationInMinutes + 5, MINUTES );
        }
        finally
        {
            service.shutdown();
            cluster.shutdown();
        }
        System.out.println( "Finish stressing, running consistency checks" );

        // Check consistency
        for ( String storeDirectory : storeDirectories )
        {
            ConsistencyCheckService.Result result = runConsistencyCheckTool( new String[]{storeDirectory},
                    System.out, System.err );
            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + storeDirectory );
            }
        }

        // Validate free id, all must be unique
        Set<Long> unusedIds = new TreeSet<>();
        for ( String storeDirectory : storeDirectories )
        {
            File idFile = new File( storeDirectory, MetaDataStore.DEFAULT_NAME + NODE_STORE_NAME + ".id" );
            IdContainer idContainer = new IdContainer( fs, idFile, 1024, true );
            idContainer.init();
            System.out.println( idFile.getAbsolutePath() + " has " + idContainer.getFreeIdCount() + " free ids" );

            long id = idContainer.getReusableId();
            while ( id != IdContainer.NO_RESULT )
            {
                assertTrue( unusedIds.add( id ) );
                id = idContainer.getReusableId();
            }

            idContainer.close( 0 );
        }

        System.out.println( "Total of " + unusedIds.size() + " reusable ids found" );

        // Cleanup disk space when everything went well
        FileUtils.deleteRecursively( clusterDirectory );
    }

    private void createInitialData( Cluster cluster ) throws Exception
    {
        for ( int i = 0; i < 1_000; i++ )
        {
            try
            {
                cluster.coreTx( ( db, tx ) ->
                {
                    for ( int j = 0; j < 1_000; j++ )
                    {
                        Node start = db.createNode();
                        Node end = db.createNode();
                        start.createRelationshipTo( end, RELATIONSHIP_TYPE );
                    }
                    tx.success();
                } );
            }
            catch ( WriteOperationsNotAllowedException e )
            {
                // skip
            }
        }
    }

    private static boolean isTransient( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof  TimeoutException ||
                e instanceof DatabaseShutdownException ||
                e instanceof TransactionFailureException ||
                e instanceof AcquireLockTimeoutException ||
                e instanceof TransientTransactionFailureException )
        {
            return true;
        }

        return isInterrupted( e.getCause() );
    }

    private static boolean isInterrupted( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof InterruptedException )
        {
            Thread.interrupted();
            return true;
        }

        return isInterrupted( e.getCause() );
    }

    private static class InsertionWorkload extends RepeatUntilCallable
    {
        private Cluster cluster;

        InsertionWorkload( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster )
        {
            super( keepGoing, onFailure );
            this.cluster = cluster;
        }

        @Override
        protected void doWork()
        {
            try
            {
                cluster.coreTx( ( db, tx ) ->
                {
                    Node nodeStart = db.createNode();
                    Node nodeEnd = db.createNode();
                    nodeStart.createRelationshipTo( nodeEnd, RELATIONSHIP_TYPE );
                    tx.success();
                } );
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "InsertionWorkload", e );
            }
        }
    }

    private static class ReelectionWorkload extends RepeatUntilCallable
    {
        private Cluster cluster;
        private final long secondsToSleep;

        ReelectionWorkload( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster, long secondsToSleep )
        {
            super( keepGoing, onFailure );
            this.cluster = cluster;
            this.secondsToSleep = secondsToSleep;
        }

        @Override
        protected void doWork()
        {
            try
            {
                CoreClusterMember leader = cluster.awaitLeader();
                leader.shutdown();
                leader.start();
                System.out.println( "Restarting leader" );
                TimeUnit.SECONDS.sleep( secondsToSleep );
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "ReelectionWorkload", e );
            }
        }
    }

    private static class DeletionWorkload extends RepeatUntilCallable
    {
        private Cluster cluster;
        private final SecureRandom rnd = new SecureRandom();
        private final int idHighRange;

        DeletionWorkload( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster, int idHighRange )
        {
            super( keepGoing, onFailure );
            this.cluster = cluster;
            this.idHighRange = idHighRange;
        }

        @Override
        protected void doWork()
        {
            try
            {
                cluster.coreTx( ( db, tx ) ->
                {
                    Node node = db.getNodeById( rnd.nextInt( idHighRange ) );
                    Iterables.stream( node.getRelationships() ).forEach( Relationship::delete );
                    node.delete();

                    tx.success();
                } );
            }
            catch ( NotFoundException e )
            {
                // Expected
            }
            catch ( Throwable e )
            {
                if ( isInterrupted( e ) || isTransient( e ) )
                {
                    // whatever let's go on with the workload
                    return;
                }

                throw new RuntimeException( "DeletionWorkload", e );
            }
        }
    }
}
