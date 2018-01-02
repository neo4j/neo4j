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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.util.Collections;
import java.util.concurrent.Executor;

import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.NullLogProvider;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class MultiPaxosContextTest
{

    @Test
    public void shouldNotConsiderInstanceJoiningWithSameIdAndIpAProblem() throws Exception
    {
        // Given
        MultiPaxosContext ctx = new MultiPaxosContext( new InstanceId( 1 ),
                10, Collections.<ElectionRole>emptyList(),
                mock( ClusterConfiguration.class ), mock( Executor.class ),
                NullLogProvider.getInstance(), new ObjectStreamFactory(),
                new ObjectStreamFactory(), mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class) );

        InstanceId joiningId = new InstanceId( 12 );
        String joiningUri = "http://127.0.0.1:900";

        // When
        ctx.getClusterContext().instanceIsJoining( joiningId, new URI( joiningUri ) );

        // Then
        assertFalse( ctx.getClusterContext().isInstanceJoiningFromDifferentUri( joiningId, new URI( joiningUri ) ));
        assertTrue( ctx.getClusterContext().isInstanceJoiningFromDifferentUri( joiningId, new URI("http://127.0.0.1:80")));
        assertFalse( ctx.getClusterContext().isInstanceJoiningFromDifferentUri( new InstanceId( 13 ), new URI( joiningUri ) ) );
    }

    @Test
    public void shouldDeepClone() throws Exception
    {
        // Given
        ObjectStreamFactory objStream = new ObjectStreamFactory();
        AcceptorInstanceStore acceptorInstances = mock( AcceptorInstanceStore.class );
        Executor executor = mock( Executor.class );
        Timeouts timeouts = mock( Timeouts.class );
        ClusterConfiguration clusterConfig = new ClusterConfiguration( "myCluster", NullLogProvider.getInstance() );
        ElectionCredentialsProvider electionCredentials = mock( ElectionCredentialsProvider.class );

        MultiPaxosContext ctx = new MultiPaxosContext( new InstanceId( 1 ),
                10, Collections.<ElectionRole>emptyList(),
                clusterConfig, executor,
                NullLogProvider.getInstance(), objStream,
                objStream, acceptorInstances, timeouts, electionCredentials );

        // When
        MultiPaxosContext snapshot = ctx.snapshot( NullLogProvider.getInstance(), timeouts, executor, acceptorInstances, objStream, objStream,
                electionCredentials );

        // Then
        assertEquals( ctx, snapshot );
    }
}
