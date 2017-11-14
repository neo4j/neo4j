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