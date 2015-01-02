/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import org.junit.Test;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerState;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstanceStore;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.impl.util.TestLogging;

import static org.mockito.Mockito.*;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.debug;

public class LearnerContextImplTest
{
    @Test
    public void shouldOnlyLogLearnMissOnce() throws Exception
    {
        // Given
        TestLogging logging = new TestLogging();
        LearnerContextImpl ctx = new LearnerContextImpl( new InstanceId( 1 ), mock(CommonContextState.class), logging,
                mock(Timeouts.class), mock( PaxosInstanceStore.class ), mock(AcceptorInstanceStore.class), mock(
                ObjectInputStreamFactory.class), mock( ObjectOutputStreamFactory.class), mock(HeartbeatContextImpl.class));

        // When
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 2l ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 2l ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l ) );

        // Then
        logging.getMessagesLog( LearnerState.class ).assertExactly(
                debug( "Did not have learned value for instance 1" ),
                debug( "Did not have learned value for instance 2" ),
                debug( "Did not have learned value for instance 1" )
        );
    }

}
