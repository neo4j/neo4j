/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.index;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class FlippableIndexContextTest
{
    @Test
    public void shouldBeAbleToSwitchDelegate() throws Exception
    {
        // GIVEN
        IndexContext actual = mock( IndexContext.class );
        IndexContext other = mock( IndexContext.class );
        FlippableIndexContext delegate = new FlippableIndexContext(actual);
        delegate.setFlipTarget( eager( other ) );

        // WHEN
        delegate.flip();
        delegate.drop();

        // THEN
        verify( other ).drop();
    }

    @Test
    public void shouldOnlySwitchDelegatesBetweenUses() throws InterruptedException
    {
        // This test ensures that is not possible to use FlippableIndexContext while a flip is in progress
        // in order to avoid nasty races (an update call still using the populator)

        // GIVEN
        final IndexContext actual = mock( IndexContext.class );
        final IndexContext other = mock( IndexContext.class );
        final FlippableIndexContext flippable = new FlippableIndexContext( actual );
        flippable.setFlipTarget( eager( other ) );
        final AtomicReference<IndexContext> result = new AtomicReference<IndexContext>();

        final CountDownLatch actualLatch = new CountDownLatch( 1 );

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                actualLatch.await();
                result.set( flippable.getDelegate() );
                return null;
            }
        } ).when( actual ).drop();

        Runnable triggerActual = new Runnable() {
            @Override
            public void run()
            {
                flippable.drop();
            }
        };

        final CountDownLatch delegateChangeLatch = new CountDownLatch( 1 );

        Runnable triggerDelegateChange = new Runnable() {
            @Override
            public void run()
            {
                try
                {
                    delegateChangeLatch.await( );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                flippable.flip( );
            }
        };

        // WHEN

        // trigger blocking call
        new Thread(triggerActual).start();

        // trigger changing delegate
        new Thread(triggerDelegateChange).start();

        delegateChangeLatch.countDown();

        // signal blocking call to finish
        actualLatch.countDown();


        // THEN
        while (result.get() == null) {}

        assertEquals( actual, result.get() );
    }
    
    private static IndexContextFactory eager( final IndexContext context )
    {
        return new IndexContextFactory()
        {
            @Override
            public IndexContext create()
            {
                return context;
            }
        };
    }
}
