package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.powermock.api.mockito.PowerMockito.mock;

public class RuleUpdateFilterIndexProxyTest
{

    private Collection<NodePropertyUpdate> lastUpdates;
    private Answer<?> saveUpdatesInLastUpdatesField = new Answer()
    {
        @Override
        public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
        {
            lastUpdates = asCollection( (Iterable<NodePropertyUpdate>) invocationOnMock.getArguments()[0] );
            return null;
        }
    };

    @Test
    public void shouldFilterUpdates() throws Exception
    {
        // Given
        IndexProxy delegate = mock(IndexProxy.class);
        when(delegate.getDescriptor()).thenReturn( new IndexDescriptor( 1337, 1337 ) );
        doAnswer( saveUpdatesInLastUpdatesField ).when(delegate).update( anyCollection() );

        RuleUpdateFilterIndexProxy indexProxy = new RuleUpdateFilterIndexProxy( delegate );

        // When I send normal updates, things work
        assertUpdates(       indexProxy, new NodePropertyUpdate( 1, 1337, null, new long[]{}, 1, new long[]{1337} ) );

        assertDoesntUpdate(  indexProxy, new NodePropertyUpdate( 1, 1336, null, new long[]{}, 1, new long[]{1337} ) );
        assertDoesntUpdate(  indexProxy, new NodePropertyUpdate( 1, 1337, null, new long[]{}, 1, new long[]{1336} ) );
    }

    @Test
    public void shouldFilterRecoveryUpdates() throws Exception
    {
        // Given
        IndexProxy delegate = mock(IndexProxy.class);
        when(delegate.getDescriptor()).thenReturn( new IndexDescriptor( 1337, 1337 ) );
        doAnswer( saveUpdatesInLastUpdatesField ).when(delegate).recover( anyCollection() );

        RuleUpdateFilterIndexProxy indexProxy = new RuleUpdateFilterIndexProxy( delegate );

        // When I send recovery updates, things work
        assertRecovers(      indexProxy, new NodePropertyUpdate( 1, 1337, null, new long[]{}, 1, new long[]{1337} ) );

        assertDoesntRecover( indexProxy, new NodePropertyUpdate( 1, 1336, null, new long[]{}, 1, new long[]{1337} ) );
        assertDoesntRecover( indexProxy, new NodePropertyUpdate( 1, 1337, null, new long[]{}, 1, new long[]{1336} ) );
    }

    private void assertRecovers( IndexProxy outer, NodePropertyUpdate update ) throws IOException
    {
        outer.recover( asList( update ) );
        assertEquals( asList( update ), lastUpdates );
    }

    private void assertDoesntRecover( IndexProxy outer, NodePropertyUpdate update ) throws IOException
    {
        outer.recover( asList( update ) );
        assertEquals( 0, lastUpdates.size() );
    }

    private void assertUpdates( IndexProxy outer, NodePropertyUpdate update ) throws IOException
    {
        outer.update( asList( update) );
        assertEquals( asList( update ), lastUpdates );
    }

    private void assertDoesntUpdate( IndexProxy outer, NodePropertyUpdate update ) throws IOException
    {
        outer.update( asList( update ) );
        assertEquals( 0, lastUpdates.size() );
    }

}
