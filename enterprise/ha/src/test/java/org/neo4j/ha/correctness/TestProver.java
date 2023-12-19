/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ha.correctness;

import org.junit.Test;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.neo4j.ha.correctness.ClusterInstance.newClusterInstance;

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
                emptySet()
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
                emptySet()
        );

        // When
        ClusterState firstState = state.performAction( new MessageDeliveryAction( Message.to( ClusterMessage.join,
                new URI( "cluster://localhost:5002" ), new Object[]{"defaultcluster",
                        new URI[]{new URI( "cluster://localhost:5003" )}} ).setHeader( Message.HEADER_CONVERSATION_ID,
                "-1" ).setHeader( Message.HEADER_FROM, "cluster://localhost:5002" ) ) );
        ClusterState secondState = state.performAction( new MessageDeliveryAction( Message.to( ClusterMessage.join,
                new URI( "cluster://localhost:5002" ), new Object[]{"defaultcluster",
                        new URI[]{new URI( "cluster://localhost:5003" )}} ).setHeader( Message.HEADER_CONVERSATION_ID,
                "-1" ).setHeader( Message.HEADER_FROM, "cluster://localhost:5002" ) ) );

        // Then
        assertEquals( firstState, secondState );
        assertEquals( firstState.hashCode(), secondState.hashCode() );
    }

}
