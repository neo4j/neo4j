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
package org.neo4j.causalclustering.scenarios;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import org.neo4j.causalclustering.core.consensus.RaftServer;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.leader_election_timeout;

public class TokenReplicationStressIT
{
    private static final int EXECUTION_TIME_SECONDS = Integer.getInteger( "TokenReplicationStressTestExecutionTimeSeconds", 30 );

    private static final String LIST_LABELS_QUERY = "CALL db.labels() " +
                                                    "YIELD label " +
                                                    "WITH label " +
                                                    "ORDER BY label " +
                                                    "RETURN collect(label) AS result";

    private static final String LIST_PROPERTY_KEYS_QUERY = "CALL db.propertyKeys() " +
                                                           "YIELD propertyKey " +
                                                           "WITH propertyKey " +
                                                           "ORDER BY propertyKey " +
                                                           "RETURN collect(propertyKey) AS result";

    private static final String LIST_RELATIONSHIP_TYPES_QUERY = "CALL db.relationshipTypes() " +
                                                                "YIELD relationshipType " +
                                                                "WITH relationshipType " +
                                                                "ORDER BY relationshipType " +
                                                                "RETURN collect(relationshipType) AS result";

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withDiscoveryServiceFactory( new SharedDiscoveryService() )
            .withSharedCoreParam( leader_election_timeout, "2s" );

    private Cluster cluster;

    @After
    public void tearDown()
    {
        if ( cluster != null )
        {
            try
            {
                cluster.shutdown();
            }
            catch ( Throwable ignore )
            {
            }
        }
    }

    @Test
    public void shouldReplicateTokensWithConcurrentElections() throws Throwable
    {
        cluster = clusterRule.startCluster();

        AtomicBoolean stop = new AtomicBoolean();

        CompletableFuture<Void> tokenCreator1 = runAsync( () -> createTokens( cluster, evenTokenIdsSupplier(), stop ) );
        CompletableFuture<Void> tokenCreator2 = runAsync( () -> createTokens( cluster, oddTokenIdsSupplier(), stop ) );
        CompletableFuture<Void> electionTrigger = runAsync( () -> triggerElections( cluster, stop ) );
        CompletableFuture<Void> allOperations = allOf( tokenCreator1, tokenCreator2, electionTrigger );

        awaitUntilDeadlineOrFailure( stop, allOperations );

        stop.set( true );
        allOperations.join();

        // assert number of tokens on every cluster member is the same after a restart
        // restart is needed to make sure tokens are persisted and not only in token caches
        cluster.shutdown();
        cluster.start();

        verifyLabelTokens( cluster );
        verifyPropertyKeyTokens( cluster );
        verifyRelationshipTypeTokens( cluster );
    }

    private static void createTokens( Cluster cluster, LongSupplier tokenIdSupplier, AtomicBoolean stop )
    {
        while ( !stop.get() )
        {
            CoreClusterMember leader = awaitLeader( cluster );
            GraphDatabaseService db = leader.database();

            // transaction that creates a lot of new tokens
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < 10; i++ )
                {
                    long tokenId = tokenIdSupplier.getAsLong();

                    Label label = Label.label( "Label_" + tokenId );
                    String propertyKey = "Property_" + tokenId;
                    RelationshipType type = RelationshipType.withName( "RELATIONSHIP_" + tokenId );

                    Node node1 = db.createNode( label );
                    Node node2 = db.createNode( label );

                    node1.setProperty( propertyKey, tokenId );
                    node2.setProperty( propertyKey, tokenId );

                    node1.createRelationshipTo( node2, type );
                }
                tx.success();
            }
            catch ( WriteOperationsNotAllowedException ignore )
            {
                // this can happen because other thread is forcing elections
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( "Failed to create tokens", t );
            }
        }
    }

    private static void triggerElections( Cluster cluster, AtomicBoolean stop )
    {
        while ( !stop.get() )
        {
            try
            {
                SECONDS.sleep( 5 );
                CoreClusterMember leaderBefore = awaitLeader( cluster );
                CoreClusterMember follower = randomClusterMember( cluster, leaderBefore );

                RaftServer leaderRaftServer = raftServerOf( leaderBefore );
                leaderRaftServer.stop();

                follower.raft().triggerElection( Clock.systemUTC() );

                SECONDS.sleep( 1 );
                leaderRaftServer.start();
                CoreClusterMember leaderAfter = awaitLeader( cluster );

                // assert that leader re-election has actually happened
                assertNotEquals( leaderBefore.id(), leaderAfter.id() );
            }
            catch ( Throwable t )
            {
                throw new RuntimeException( "Failed to trigger an election", t );
            }
        }
    }

    private static void awaitUntilDeadlineOrFailure( AtomicBoolean stop, CompletableFuture<Void> allOperations ) throws InterruptedException
    {
        Duration executionTime = Duration.ofSeconds( EXECUTION_TIME_SECONDS );
        LocalTime deadline = LocalTime.now().plus( executionTime );

        while ( deadline.compareTo( LocalTime.now() ) > 0 )
        {
            if ( allOperations.isCompletedExceptionally() )
            {
                stop.set( true );
                break;
            }
            SECONDS.sleep( 1 );
        }
    }

    private static CoreClusterMember awaitLeader( Cluster cluster )
    {
        try
        {
            return cluster.awaitLeader();
        }
        catch ( TimeoutException e )
        {
            throw new IllegalStateException( "No leader found", e );
        }
    }

    private static RaftServer raftServerOf( CoreClusterMember leader )
    {
        return leader.database().getDependencyResolver().resolveDependency( RaftServer.class );
    }

    private static CoreClusterMember randomClusterMember( Cluster cluster, CoreClusterMember except )
    {
        CoreClusterMember[] members = cluster.coreMembers()
                .stream()
                .filter( member -> !member.id().equals( except.id() ) )
                .toArray( CoreClusterMember[]::new );

        return members[ThreadLocalRandom.current().nextInt( members.length )];
    }

    private static void verifyLabelTokens( Cluster cluster )
    {
        verifyTokens( "Labels", cluster, LIST_LABELS_QUERY );
    }

    private static void verifyPropertyKeyTokens( Cluster cluster )
    {
        verifyTokens( "Property keys", cluster, LIST_PROPERTY_KEYS_QUERY );
    }

    private static void verifyRelationshipTypeTokens( Cluster cluster )
    {
        verifyTokens( "Relationship types", cluster, LIST_RELATIONSHIP_TYPES_QUERY );
    }

    @SuppressWarnings( "unchecked" )
    private static void verifyTokens( String tokenType, Cluster cluster, String listTokensQuery )
    {
        List<List<String>> tokensFromAllMembers = cluster.coreMembers()
                .stream()
                .map( member -> member.database().execute( listTokensQuery ) )
                .map( Iterators::single )
                .map( record -> record.get( "result" ) )
                .map( value -> (List<String>) value )
                .collect( toList() );

        for ( List<String> tokens1 : tokensFromAllMembers )
        {
            for ( List<String> tokens2 : tokensFromAllMembers )
            {
                if ( !tokens1.equals( tokens2 ) )
                {
                    String tokensString = tokensFromAllMembers.stream()
                            .map( List::toString )
                            .collect( joining( "\n" ) );

                    fail( tokenType + " are not the same on different cluster members:\n" + tokensString );
                }
            }
        }
    }

    private static LongSupplier evenTokenIdsSupplier()
    {
        MutableInt id = new MutableInt();
        return () -> id.getAndAdd( 2 );
    }

    private static LongSupplier oddTokenIdsSupplier()
    {
        MutableInt id = new MutableInt( 1 );
        return () -> id.getAndAdd( 2 );
    }
}
