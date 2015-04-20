/*
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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.PaxosInstanceStore;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class LearnerContextImplTest
{
    @Test
    public void shouldOnlyLogLearnMissOnce() throws Exception
    {
        // Given
        Logging logging = mock( Logging.class );
        LearnerContextImpl ctx = new LearnerContextImpl( new InstanceId( 1 ), mock( CommonContextState.class ),
                logging, mock( Timeouts.class ), mock( PaxosInstanceStore.class ), mock( AcceptorInstanceStore.class ),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( HeartbeatContextImpl.class ) );

        final List<String> logs = new ArrayList<>();
        ConsoleLogger consoleLogger = new ConsoleLogger( null )
        {
            @Override
            public void log( String message )
            {
                logs.add( message );
            }
        };
        doReturn( consoleLogger ).when( logging ).getConsoleLog( Matchers.<Class> any() );

        // When
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 2l ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 2l ) );
        ctx.notifyLearnMiss( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l ) );

        // Then
        assertEquals( 3, logs.size() );
        assertTrue( logs.get( 0 ).startsWith( "Did not have learned value for Paxos instance 1." ) );
        assertTrue( logs.get( 1 ).startsWith( "Did not have learned value for Paxos instance 2." ) );
        assertTrue( logs.get( 2 ).startsWith( "Did not have learned value for Paxos instance 1." ) );
    }

}
