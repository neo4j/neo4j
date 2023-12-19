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

import java.io.File;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.neo4j.ha.correctness.ClusterInstance.newClusterInstance;

public class Prover
{
    private final Queue<ClusterState> unexploredKnownStates = new LinkedList<>();
    private final ProofDatabase db = new ProofDatabase( "./clusterproof" );

    public static void main( String... args ) throws Exception
    {
        new Prover().prove();
    }

    public void prove() throws Exception
    {
        try
        {
            System.out.println( "Bootstrap genesis state.." );
            bootstrapCluster();
            System.out.println( "Begin exploring delivery orders." );
            exploreUnexploredStates();
            System.out.println( "Exporting graphviz.." );
            db.export( new GraphVizExporter( new File( "./proof.gs" ) ) );
        }
        finally
        {
            db.shutdown();
        }

        // Generate .svg :
        // dot -Tsvg proof.gs -o proof.svg
    }

    private void bootstrapCluster() throws Exception
    {
        String instance1 = "cluster://localhost:5001";
        String instance2 = "cluster://localhost:5002";
        String instance3 = "cluster://localhost:5003";
        ClusterConfiguration config = new ClusterConfiguration( "default",
                NullLogProvider.getInstance(),
                instance1,
                instance2,
                instance3 );

        ClusterState state = new ClusterState(
                asList(
                        newClusterInstance( new InstanceId( 1 ), new URI( instance1 ), new Monitors(), config,
                                10, NullLogProvider.getInstance() ),
                        newClusterInstance( new InstanceId( 2 ), new URI( instance2 ), new Monitors(), config,
                                10, NullLogProvider.getInstance() ),
                        newClusterInstance( new InstanceId( 3 ), new URI( instance3 ), new Monitors(), config,
                                10, NullLogProvider.getInstance() ) ),
                emptySet()
        );

        state = state.performAction( new MessageDeliveryAction( Message.to( ClusterMessage.create,
                new URI( instance3 ), "defaultcluster" ).setHeader( Message.HEADER_CONVERSATION_ID,
                "-1" ).setHeader( Message.HEADER_FROM, instance3 ) ) );
        state = state.performAction( new MessageDeliveryAction( Message.to( ClusterMessage.join,
                new URI( instance2 ), new Object[]{"defaultcluster", new URI[]{new URI( instance3 )}} ).setHeader(
                Message.HEADER_CONVERSATION_ID, "-1" ).setHeader( Message.HEADER_FROM, instance2 ) ) );
        state = state.performAction( new MessageDeliveryAction( Message.to( ClusterMessage.join,
                new URI( instance1 ), new Object[]{"defaultcluster", new URI[]{new URI( instance3 )}} ).setHeader(
                Message.HEADER_CONVERSATION_ID, "-1" ).setHeader( Message.HEADER_FROM, instance1 ) ) );

        state.addPendingActions( new InstanceCrashedAction( instance3 ) );

        unexploredKnownStates.add( state );

        db.newState( state );
    }

    private void exploreUnexploredStates()
    {
        while ( !unexploredKnownStates.isEmpty() )
        {
            ClusterState state = unexploredKnownStates.poll();

            Iterator<Pair<ClusterAction, ClusterState>> newStates = state.transitions();
            while ( newStates.hasNext() )
            {
                Pair<ClusterAction, ClusterState> next = newStates.next();
                System.out.println( db.numberOfKnownStates() + " (" + unexploredKnownStates.size() + ")" );

                ClusterState nextState = next.other();
                if ( !db.isKnownState( nextState ) )
                {
                    db.newStateTransition( state, next );
                    unexploredKnownStates.offer( nextState );

                    if ( nextState.isDeadEnd() )
                    {
                        System.out.println( "DEAD END: " + nextState.toString() + " (" + db.id( nextState ) + ")" );
                    }
                }
            }
        }
    }
}
