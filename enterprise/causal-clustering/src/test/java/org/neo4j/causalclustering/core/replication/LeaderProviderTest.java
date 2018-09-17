/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core.replication;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaderProviderTest
{

    private static final MemberId MEMBER_ID = new MemberId( UUID.randomUUID() );
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final LeaderProvider leaderProvider = new LeaderProvider();

    @Before
    public void before()
    {
        leaderProvider.setLeader( null );
    }

    @Test
    public void shouldGiveCurrentLeaderIfAvailable() throws InterruptedException
    {
        leaderProvider.setLeader( MEMBER_ID );
        assertEquals( leaderProvider.currentLeader(), MEMBER_ID );
        assertEquals( leaderProvider.awaitLeader(), MEMBER_ID );
    }

    @Test
    public void shouldWaitForNonNullValue() throws InterruptedException, ExecutionException, TimeoutException
    {
        // given
        int threads = 3;
        assertNull( leaderProvider.currentLeader() );

        // when
        CompletableFuture<ArrayList<MemberId>> futures = CompletableFuture.completedFuture( new ArrayList<>() );
        for ( int i = 0; i < threads; i++ )
        {
            CompletableFuture<MemberId> future = CompletableFuture.supplyAsync( getCurrentLeader(), executorService );
            futures = futures.thenCombine( future, ( completableFutures, memberId ) ->
            {
                completableFutures.add( memberId );
                return completableFutures;
            } );
        }

        // then
        Thread.sleep( 100 );
        assertFalse( futures.isDone() );

        // when
        leaderProvider.setLeader( MEMBER_ID );

        ArrayList<MemberId> memberIds = futures.get( 5, TimeUnit.SECONDS );

        // then
        assertTrue( memberIds.stream().allMatch( memberId -> memberId.equals( MEMBER_ID ) ) );
    }

    private Supplier<MemberId> getCurrentLeader()
    {
        return () ->
        {
            try
            {
                return leaderProvider.awaitLeader();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( "Interrupted" );
            }
        };
    }
}
