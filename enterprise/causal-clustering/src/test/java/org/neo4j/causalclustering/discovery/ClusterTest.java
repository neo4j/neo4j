/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterTest
{
    @Test
    public void shouldShutdownAllMembersThenThrowException() throws Exception
    {
        // given
        Cluster clusterMock = mock( Cluster.class );
        CoreClusterMember unhealthyCore = mock( CoreClusterMember.class );
        doThrow( IllegalStateException.class ).when( unhealthyCore ).shutdown();
        CoreClusterMember healthyCore = mock( CoreClusterMember.class );
        ReadReplica unhealthyReadReplica = mock( ReadReplica.class );
        doThrow( IllegalStateException.class ).when( unhealthyReadReplica ).shutdown();
        ReadReplica healthyReadReplica = mock( ReadReplica.class );

        doCallRealMethod().when( clusterMock ).shutdown();
        when( clusterMock.coreMembers() ).thenReturn( Arrays.asList( unhealthyCore, healthyCore ) );
        when( clusterMock.readReplicas() ).thenReturn( Arrays.asList( unhealthyReadReplica, healthyReadReplica ) );

        // when
        RuntimeException exception = null;
        try
        {
            clusterMock.shutdown();
        }
        catch ( RuntimeException e )
        {
            exception = e;
        }

        // then
        assertNotNull( exception );
        verify( healthyCore, only() ).shutdown();
        verify( unhealthyCore, only() ).shutdown();
        verify( healthyReadReplica, only() ).shutdown();
        verify( unhealthyReadReplica, only() ).shutdown();

        assertEquals( IllegalStateException.class, exception.getCause().getCause().getClass() );
        assertEquals( 1, exception.getSuppressed().length );
        assertEquals( IllegalStateException.class, exception.getSuppressed()[0].getCause().getClass() );
    }
}
