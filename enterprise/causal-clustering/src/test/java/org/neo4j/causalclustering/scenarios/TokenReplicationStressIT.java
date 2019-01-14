/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.leader_election_timeout;
import static org.neo4j.causalclustering.core.RaftServerModule.RAFT_SERVER_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TokenReplicationStressIT
{
    private static final int EXECUTION_TIME_SECONDS = Integer.getInteger( "TokenReplicationStressTestExecutionTimeSeconds", 30 );

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withDiscoveryServiceType( DiscoveryServiceType.SHARED )
            .withSharedCoreParam( leader_election_timeout, "2s" );

    private Cluster cluster;

    @After
    public void tearDown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
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

        verifyTokens( cluster );

        // assert number of tokens on every cluster member is the same after a restart
        // restart is needed to make sure tokens are persisted and not only in token caches
        cluster.shutdown();
        cluster.start();
        verifyTokens( cluster );
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
                CoreClusterMember leader = awaitLeader( cluster );
                CoreClusterMember follower = randomClusterMember( cluster, leader );

                // make the current leader unresponsive
                Server raftServer = raftServer( leader );
                raftServer.stop();

                // trigger an election and await until a new leader is elected
                follower.raft().triggerElection( Clock.systemUTC() );
                assertEventually( "Leader re-election did not happen", () -> awaitLeader( cluster ), not( equalTo( leader ) ), 1, MINUTES );

                // make the previous leader responsive again
                raftServer.start();
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

    private static CoreClusterMember randomClusterMember( Cluster cluster, CoreClusterMember except )
    {
        CoreClusterMember[] members = cluster.coreMembers()
                .stream()
                .filter( member -> !member.id().equals( except.id() ) )
                .toArray( CoreClusterMember[]::new );

        return members[ThreadLocalRandom.current().nextInt( members.length )];
    }

    private void verifyTokens( Cluster cluster )
    {
        verifyLabelTokens( cluster );
        verifyPropertyKeyTokens( cluster );
        verifyRelationshipTypeTokens( cluster );
    }

    private void verifyLabelTokens( Cluster cluster )
    {
        verifyTokens( "Labels", cluster, this::allLabels );
    }

    private void verifyPropertyKeyTokens( Cluster cluster )
    {
        verifyTokens( "Property keys", cluster, this::allPropertyKeys );
    }

    private void verifyRelationshipTypeTokens( Cluster cluster )
    {
        verifyTokens( "Relationship types", cluster, this::allRelationshipTypes );
    }

    private static void verifyTokens( String tokenType, Cluster cluster, Function<CoreClusterMember,List<String>> tokensExtractor )
    {
        List<List<String>> tokensFromAllMembers = cluster.coreMembers()
                .stream()
                .map( tokensExtractor )
                .collect( toList() );

        for ( List<String> tokens : tokensFromAllMembers )
        {
            assertTokensAreUnique( tokens );
        }

        if ( !allTokensEqual( tokensFromAllMembers ) )
        {
            String tokensString = tokensFromAllMembers.stream()
                    .map( List::toString )
                    .collect( joining( "\n" ) );

            fail( tokenType + " are not the same on different cluster members:\n" + tokensString );
        }
    }

    private static void assertTokensAreUnique( List<String> tokens )
    {
        Set<String> uniqueTokens = new HashSet<>( tokens );
        if ( uniqueTokens.size() != tokens.size() )
        {
            fail( "Tokens contain duplicates: " + tokens );
        }
    }

    private static boolean allTokensEqual( List<List<String>> tokensFromAllMembers )
    {
        long distinctSets = tokensFromAllMembers.stream()
                .map( HashSet::new )
                .distinct()
                .count();

        return distinctSets == 1;
    }

    private List<String> allLabels( CoreClusterMember member )
    {
        return allTokens( member, TokenAccess.LABELS )
                .stream()
                .map( Label::name )
                .collect( toList() );
    }

    private List<String> allPropertyKeys( CoreClusterMember member )
    {
        return allTokens( member, TokenAccess.PROPERTY_KEYS );
    }

    private List<String> allRelationshipTypes( CoreClusterMember member )
    {
        return allTokens( member, TokenAccess.RELATIONSHIP_TYPES )
                .stream()
                .map( RelationshipType::name )
                .collect( toList() );
    }

    private static <T> List<T> allTokens( CoreClusterMember member, TokenAccess<T> tokenAccess )
    {
        CoreGraphDatabase db = member.database();
        try ( Transaction ignore = db.beginTx() )
        {
            KernelTransaction kernelTx = currentKernelTx( member );
            return Iterators.asList( tokenAccess.all( kernelTx ) );
        }
    }

    private static LongSupplier evenTokenIdsSupplier()
    {
        return tokenIdsSupplier( 0 );
    }

    private static LongSupplier oddTokenIdsSupplier()
    {
        return tokenIdsSupplier( 1 );
    }

    private static LongSupplier tokenIdsSupplier( long initialValue )
    {
        return LongStream.iterate( initialValue, i -> i + 2 ).iterator()::nextLong;
    }

    private static Server raftServer( CoreClusterMember member )
    {
        return member.database().getDependencyResolver().resolveDependency( Server.class, new RaftServerSelectionStrategy() );
    }

    private static KernelTransaction currentKernelTx( CoreClusterMember member )
    {
        ThreadToStatementContextBridge bridge = member.database().getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        return bridge.getKernelTransactionBoundToThisThread( true );
    }

    private static class RaftServerSelectionStrategy implements DependencyResolver.SelectionStrategy
    {
        @Override
        public <T> T select( Class<T> type, Iterable<? extends T> candidates ) throws IllegalArgumentException
        {
            assertEquals( Server.class, type );
            return Iterables.stream( candidates )
                    .map( Server.class::cast )
                    .filter( server -> RAFT_SERVER_NAME.equals( server.name() ) )
                    .findFirst()
                    .map( type::cast )
                    .orElseThrow( IllegalStateException::new );
        }
    }
}
