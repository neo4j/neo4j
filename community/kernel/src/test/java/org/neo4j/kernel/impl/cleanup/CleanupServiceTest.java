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
package org.neo4j.kernel.impl.cleanup;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.Thunks.TRUE;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Thunk;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

public class CleanupServiceTest
{
    @Test
    public void shouldScheduleCleanupTaskWhenStarted() throws Throwable
    {
        // GIVEN
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, mock( Logging.class ),
                referenceQueue, TRUE );

        verifyZeroInteractions( scheduler );

        // WHEN
        service.start();

        // THEN
        verify( scheduler ).scheduleRecurring( Matchers.<Runnable> any(), anyLong(), any( TimeUnit.class ) );
    }

    @Test
    public void shouldCleanupCollectedReferences() throws Throwable
    {
        // GIVEN
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, mock( Logging.class ),
                referenceQueue, TRUE );
        service.start();
        Runnable cleanupTask = acquireCleanupTask();
        CleanupReference collectedReference = mock(CleanupReference.class);
        when( referenceQueue.remove() ).thenReturn( collectedReference ).thenReturn(null);

        // WHEN
        cleanupTask.run();

        // THEN
        verify( collectedReference, times( 1 ) ).cleanupNow(false);
    }

    @Test
    public void shouldInvokeHandlerWhenCleaningUpReferenceManually() throws Throwable
    {
        // GIVEN
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, mock( Logging.class ),
                referenceQueue, TRUE );
        service.start();
        Closeable handler = mock(Closeable.class);
        ResourceIterator<Object> reference = service.resourceIterator( new StubIterator(), handler );

        // WHEN
        reference.close();

        // THEN
        verify( handler, times( 1 ) ).close();
    }

    @Test
    public void shouldInvokeHandlerWhenCleaningUpCollectedReference() throws Throwable
    {
        // GIVEN
        Logging logging = mock(Logging.class);
        StringLogger logger = mock(StringLogger.class);
        when(logging.getMessagesLog(any(Class.class))).thenReturn(logger);
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, logging, referenceQueue, TRUE );
        service.start();
        Runnable cleanupTask = acquireCleanupTask();
        Closeable handler = mock( Closeable.class );
        ResourceIterator<Object> reference = service.resourceIterator( new StubIterator(), handler );
        when( referenceQueue.remove() )
                .thenReturn(((AutoCleanupResourceIterator) reference).cleanup)
                .thenReturn(null);

        // WHEN
        cleanupTask.run();

        // THEN
        verify( handler, times( 1 ) ).close();
        verify(logger).warn("Resource not closed: AutoCleanupResourceIterator(StubIterator())");
    }

    @Test
    public void shouldOnlyInvokeHandlerOnce() throws Throwable
    {
        // GIVEN
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, mock( Logging.class ),
                referenceQueue, TRUE );
        service.start();
        Runnable cleanupTask = acquireCleanupTask();
        Closeable handler = mock( Closeable.class );
        ResourceIterator<Object> reference = service.resourceIterator( new StubIterator(), handler );
        when( referenceQueue.remove() )
                .thenReturn(((AutoCleanupResourceIterator) reference).cleanup)
                .thenReturn(null);

        reference.close();

        // WHEN
        cleanupTask.run();

        // THEN
        verify( handler, times( 1 ) ).close();
    }

    @Test
    public void shouldCleanUpWhenEmptyIteratorIsDepleted() throws Throwable
    {
        // GIVEN
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, mock( Logging.class ),
                referenceQueue, TRUE );
        service.start();
        Closeable handler = mock(Closeable.class);
        ResourceIterator<Object> reference = service.resourceIterator( new StubIterator(), handler );

        // WHEN
        assertFalse(reference.hasNext());

        // THEN
        verify(handler, times(1)).close();
    }

    @Test
    public void shouldCleanUpWhenFilledIteratorIsDepleted() throws Throwable
    {
        // GIVEN
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, mock( Logging.class ),
                referenceQueue, TRUE );
        service.start();
        Closeable handler = mock( Closeable.class );
        ResourceIterator<Object> reference = service.resourceIterator( new StubIterator( 1, 2, 3 ), handler );

        // WHEN
        while( reference.hasNext() )
            reference.next();

        // THEN
        verify(handler, times(1)).close();
    }

    @Test
    public void shouldNotCleanUpIfIteratorIsNotDepleted() throws Throwable
    {
        // GIVEN
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, mock( Logging.class ),
                referenceQueue, TRUE );
        service.start();
        Closeable handler = mock( Closeable.class );
        ResourceIterator<Object> reference = service.resourceIterator( new StubIterator( 1, 2, 3 ), handler );

        // WHEN
        while( reference.hasNext() )
        {
            if ( reference.next().equals(2) )
                break;
        }

        // THEN
        verify(handler, never()).close();
    }

    @Test
    public void shouldSkipCleanupRegistrationIfNotNecessary() throws Throwable
    {
        // GIVEN
        @SuppressWarnings( "unchecked" )
        Thunk<Boolean> cleanupNecessity = mock( Thunk.class );
        when( cleanupNecessity.evaluate() ).thenReturn( false );
        Logging logging = mock( Logging.class );
        when( logging.getMessagesLog( Matchers.<Class<?>> any() ) ).thenReturn( DEV_NULL );
        CleanupService service = new ReferenceQueueBasedCleanupService( scheduler, logging,
                referenceQueue, cleanupNecessity );
        service.start();
        Iterator<Integer> iterator = new ArrayIterator<Integer>( new Integer[] { 1, 2, 3 } );

        // WHEN
        Closeable closer = mock( Closeable.class );
        ResourceIterator<Integer> resourceIterator = service.resourceIterator( iterator, closer );
        resourceIterator.next(); // Don't exhaust it
        service.stop();

        // THEN
        verify( cleanupNecessity, times( 1 ) ).evaluate();
        verifyZeroInteractions( closer );
    }

    private final JobScheduler scheduler = mock( JobScheduler.class );
    private final CleanupReferenceQueue referenceQueue = mock( CleanupReferenceQueue.class );

    private Runnable acquireCleanupTask()
    {
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass( Runnable.class );
        verify( scheduler ).scheduleRecurring( taskCaptor.capture(), anyLong(), any( TimeUnit.class ) );
        Runnable task = taskCaptor.getValue();
        return task;
    }

    private static class StubIterator implements Iterator<Object>
    {
        private final int[] content;
        private int i;

        StubIterator(int... content)
        {
            this.content = content;
        }

        @Override
        public boolean hasNext() {
            return i < content.length;
        }

        @Override
        public Object next() {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return content[i++];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "StubIterator()";
        }
    }
}