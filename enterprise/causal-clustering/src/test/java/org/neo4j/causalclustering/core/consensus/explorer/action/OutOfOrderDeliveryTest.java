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
package org.neo4j.causalclustering.core.consensus.explorer.action;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Election;
import org.neo4j.causalclustering.core.consensus.RaftMessages.Timeout.Heartbeat;
import org.neo4j.causalclustering.core.consensus.explorer.ClusterState;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class OutOfOrderDeliveryTest
{
    @Test
    public void shouldReOrder() throws Exception
    {
        // given
        ClusterState clusterState = new ClusterState( asSet( member( 0 ) ) );
        clusterState.queues.get( member( 0 ) ).add( new Election( member( 0 ) ) );
        clusterState.queues.get( member( 0 ) ).add( new Heartbeat( member( 0 ) ) );

        // when
        ClusterState reOrdered = new OutOfOrderDelivery( member( 0 ) ).advance( clusterState );

        // then
        assertEquals( new Heartbeat( member( 0 ) ), reOrdered.queues.get( member( 0 ) ).poll() );
        assertEquals( new Election( member( 0 ) ), reOrdered.queues.get( member( 0 ) ).poll() );
    }
}
