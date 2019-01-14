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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.concurrent.Executor;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiPaxosContextTest
{
    @Test
    public void shouldNotConsiderInstanceJoiningWithSameIdAndIpAProblem() throws Exception
    {
        // Given

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext ctx = new MultiPaxosContext( new InstanceId( 1 ),
                Collections.emptyList(),
                mock( ClusterConfiguration.class ), mock( Executor.class ),
                NullLogProvider.getInstance(), new ObjectStreamFactory(),
                new ObjectStreamFactory(), mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class), config );

        InstanceId joiningId = new InstanceId( 12 );
        String joiningUri = "http://127.0.0.1:900";

        // When
        ctx.getClusterContext().instanceIsJoining( joiningId, new URI( joiningUri ) );

        // Then
        assertFalse( ctx.getClusterContext().isInstanceJoiningFromDifferentUri( joiningId, new URI( joiningUri ) ) );
        assertTrue( ctx.getClusterContext()
                .isInstanceJoiningFromDifferentUri( joiningId, new URI( "http://127.0.0.1:80" ) ) );
        assertFalse( ctx.getClusterContext()
                .isInstanceJoiningFromDifferentUri( new InstanceId( 13 ), new URI( joiningUri ) ) );
    }

    @Test
    public void shouldDeepClone()
    {
        // Given
        ObjectStreamFactory objStream = new ObjectStreamFactory();
        AcceptorInstanceStore acceptorInstances = mock( AcceptorInstanceStore.class );
        Executor executor = mock( Executor.class );
        Timeouts timeouts = mock( Timeouts.class );
        ClusterConfiguration clusterConfig = new ClusterConfiguration( "myCluster", NullLogProvider.getInstance() );
        ElectionCredentialsProvider electionCredentials = mock( ElectionCredentialsProvider.class );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext ctx = new MultiPaxosContext( new InstanceId( 1 ),
                Collections.emptyList(),
                clusterConfig, executor,
                NullLogProvider.getInstance(), objStream,
                objStream, acceptorInstances, timeouts, electionCredentials, config );

        // When
        MultiPaxosContext snapshot = ctx.snapshot( NullLogProvider.getInstance(), timeouts, executor, acceptorInstances,
                objStream, objStream, electionCredentials );

        // Then
        assertEquals( ctx, snapshot );
    }
}
