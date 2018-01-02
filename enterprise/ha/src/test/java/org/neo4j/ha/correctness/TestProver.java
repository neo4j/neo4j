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
package org.neo4j.ha.correctness;

import java.net.URI;

import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;
import static org.neo4j.ha.correctness.ClusterInstance.newClusterInstance;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;

public class TestProver
{

    @Test
    public void aClusterSnapshotShouldEqualItsOrigin() throws Exception
    {
        // Given
        ClusterConfiguration config = new ClusterConfiguration( "default",
                NullLogProvider.getInstance(),
                "cluster://localhost:5001",
                "cluster://localhost:5002",
                "cluster://localhost:5003" );

        ClusterState state = new ClusterState(
                asList(
                        newClusterInstance( new InstanceId( 1 ), new URI( "cluster://localhost:5001" ),
                                new Monitors(), config, 10, NullLogProvider.getInstance() ),
                        newClusterInstance( new InstanceId( 2 ), new URI( "cluster://localhost:5002" ),
                                new Monitors(), config, 10, NullLogProvider.getInstance() ),
                        newClusterInstance( new InstanceId( 3 ), new URI( "cluster://localhost:5003" ),
                                new Monitors(), config, 10, NullLogProvider.getInstance() ) ),
                emptySetOf( ClusterAction.class )
        );

        // When
        ClusterState snapshot = state.snapshot();

        // Then
        assertEquals( state, snapshot );
        assertEquals( state.hashCode(), snapshot.hashCode() );
    }

    @Test
    public void twoStatesWithSameSetupAndPendingMessagesShouldBeEqual() throws Exception
    {
        // Given
        ClusterConfiguration config = new ClusterConfiguration( "default",
                NullLogProvider.getInstance(),
                "cluster://localhost:5001",
                "cluster://localhost:5002",
                "cluster://localhost:5003" );

        ClusterState state = new ClusterState(
                asList(
                        newClusterInstance( new InstanceId( 1 ), new URI( "cluster://localhost:5001" ),
                                new Monitors(), config, 10, NullLogProvider.getInstance() ),
                        newClusterInstance( new InstanceId( 2 ), new URI( "cluster://localhost:5002" ),
                                new Monitors(), config, 10, NullLogProvider.getInstance() ),
                        newClusterInstance( new InstanceId( 3 ), new URI( "cluster://localhost:5003" ),
                                new Monitors(), config, 10, NullLogProvider.getInstance() ) ),
                emptySetOf( ClusterAction.class )
        );

        // When
        ClusterState firstState = state.performAction( new MessageDeliveryAction( Message.to( ClusterMessage.join,
                new URI( "cluster://localhost:5002" ), new Object[]{"defaultcluster",
                        new URI[]{new URI( "cluster://localhost:5003" )}} ).setHeader( Message.CONVERSATION_ID,
                "-1" ).setHeader( Message.FROM, "cluster://localhost:5002" ) ) );
        ClusterState secondState = state.performAction( new MessageDeliveryAction( Message.to( ClusterMessage.join,
                new URI( "cluster://localhost:5002" ), new Object[]{"defaultcluster",
                        new URI[]{new URI( "cluster://localhost:5003" )}} ).setHeader( Message.CONVERSATION_ID,
                "-1" ).setHeader( Message.FROM, "cluster://localhost:5002" ) ) );

        // Then
        assertEquals( firstState, secondState );
        assertEquals( firstState.hashCode(), secondState.hashCode() );
    }

}
