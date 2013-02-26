package org.neo4j.kernel.impl.api.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class FlippingIndexContextTest
{
    @Test
    public void shouldFlipOverToOnlineIndexContext() throws Exception
    {
        // GIVEN
        MockedPopulatingIndexContext populatingContext = new MockedPopulatingIndexContext();
        IndexContext onlineContext = mock( IndexContext.class );
        final AtomicDelegatingIndexContext delegatingContext = new AtomicDelegatingIndexContext( populatingContext );
        Flipper flipper = new Flipper( delegatingContext, onlineContext );
        populatingContext.setFlipper( flipper );
        final AtomicBoolean actionRun = new AtomicBoolean( false );
        Runnable action = new Runnable()
        {
            @Override
            public void run()
            {
                assertEquals( BlockingIndexContext.class, delegatingContext.getDelegate().getClass() );
                actionRun.set( true );
            }
        };

        // WHEN
        populatingContext.triggerFlip( action );

        // THEN
        assertTrue( actionRun.get() );
        assertEquals( onlineContext, delegatingContext.getDelegate() );
    }
    
    private static class MockedPopulatingIndexContext extends IndexContext.Adapter implements FlipAwareIndexContext
    {
        private Flipper flipper;

        @Override
        public void setFlipper( Flipper flipper )
        {
            this.flipper = flipper;
        }
        
        void triggerFlip( Runnable action )
        {
            this.flipper.flip( action );
        }

        @Override
        public IndexRule getIndexRule()
        {
            throw new UnsupportedOperationException(  );
        }
    }
}
