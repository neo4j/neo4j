/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.fabric.stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.fabric.config.FabricConfig;

public class Prefetcher
{
    private static final RecordOrError END = new RecordOrError( null, null );

    private final FabricConfig.DataStream streamConfig;
    private final List<PrefetchOperator> prefetchOperators = new ArrayList<>();

    public Prefetcher( FabricConfig.DataStream streamConfig )
    {
        this.streamConfig = streamConfig;
    }

    public synchronized Flux<Record> addPrefetch( Flux<Record> recordStream )
    {
        // Every time a new operator is added, the desired buffer size for each operator is recomputed,
        // so the configured buffer size is equally shared among the operators.
        // Note that operator buffer size is a soft limit, since decreasing it,
        // does not remove already queued records that are over the new limit
        var operatorsCount = prefetchOperators.size() + 1;
        var newLowWatermark = computeLowWatermark( operatorsCount );
        var newHighWatermark = computeHighWatermark( operatorsCount );
        updateWatermarks( newLowWatermark, newHighWatermark );
        var prefetchOperator = new PrefetchOperator( recordStream, newLowWatermark, newHighWatermark );
        prefetchOperators.add( prefetchOperator );
        return prefetchOperator;
    }

    private int computeHighWatermark( int operatorsCount )
    {
        return Math.max( 1, streamConfig.getBufferSize() / operatorsCount );
    }

    private int computeLowWatermark( int operatorsCount )
    {
        return streamConfig.getBufferLowWatermark() / operatorsCount;
    }

    private void updateWatermarks( int lowWatermark, int highWatermark )
    {
        prefetchOperators.forEach( prefetchOperator ->
        {
            prefetchOperator.bufferLowWatermark = lowWatermark;
            prefetchOperator.bufferHighWatermark = highWatermark;
        } );
    }

    private synchronized void removeOperator( PrefetchOperator operator )
    {
        prefetchOperators.remove( operator );

        if ( !prefetchOperators.isEmpty() )
        {
            var operatorsCount = prefetchOperators.size();
            var newLowWatermark = computeLowWatermark( operatorsCount );
            var newHighWatermark = computeHighWatermark( operatorsCount );
            updateWatermarks( newLowWatermark, newHighWatermark );
        }
    }

    private class PrefetchOperator extends FluxOperator<Record, Record>
    {
        private final Queue<RecordOrError> buffer;
        private final RecordSubscriber upstreamSubscriber;
        private final AtomicBoolean producing = new AtomicBoolean( false );
        private final AtomicLong pendingRequested = new AtomicLong( 0 );
        private volatile int bufferLowWatermark;
        private volatile int bufferHighWatermark;
        private volatile boolean finished;
        private volatile Subscriber<Record> downstreamSubscriber;

        PrefetchOperator( Flux<Record> recordStream, int bufferLowWatermark, int bufferHighWatermark )
        {
            super( recordStream );
            this.bufferHighWatermark = bufferHighWatermark;
            this.bufferLowWatermark = bufferLowWatermark;
            buffer = new ArrayBlockingQueue<>( streamConfig.getBufferSize() + 1 );
            this.upstreamSubscriber = new RecordSubscriber();
            recordStream.subscribeWith( upstreamSubscriber );
        }

        private void maybeRequest()
        {
            int buffered = buffer.size();
            long pendingRequested = upstreamSubscriber.pendingRequested.get();
            long batchSize = bufferHighWatermark - buffered - pendingRequested;
            if ( buffered + pendingRequested <= bufferLowWatermark
                    // computed batch size can be 0 if low watermark equals high watermark
                    && batchSize != 0 )
            {
                upstreamSubscriber.request( batchSize );
            }
        }

        @Override
        public void subscribe( CoreSubscriber subscriber )
        {
            this.downstreamSubscriber = subscriber;
            subscriber.onSubscribe( new Subscription()
            {
                @Override
                public void request( long l )
                {
                    pendingRequested.addAndGet( l );
                    maybeProduce();
                }

                @Override
                public void cancel()
                {
                    finish();
                    upstreamSubscriber.close();
                }
            } );
        }

        private void maybeProduce()
        {

            if ( buffer.peek() == null || downstreamSubscriber == null || pendingRequested.get() == 0 || finished )
            {
                return;
            }

            if ( !producing.compareAndSet( false, true ) )
            {
                return;
            }

            while ( !finished && pendingRequested.get() > 0 )
            {
                RecordOrError recordOrError = buffer.poll();
                if ( recordOrError == null )
                {
                    break;
                }

                if ( recordOrError == END )
                {
                    downstreamSubscriber.onComplete();
                    finish();
                    break;
                }

                if ( recordOrError.error != null )
                {
                    downstreamSubscriber.onError( recordOrError.error );
                    finish();
                    break;
                }

                pendingRequested.decrementAndGet();
                downstreamSubscriber.onNext( recordOrError.record );
            }

            maybeRequest();

            producing.set( false );
            // re-check that nothing changed between last check of the buffer state and setting 'producing' flag to 'false'
            maybeProduce();
        }

        private void finish()
        {
            finished = true;
            removeOperator( this );
        }

        private class RecordSubscriber implements Subscriber<Record>
        {

            private volatile Subscription subscription;
            private final AtomicLong pendingRequested = new AtomicLong( 0 );

            @Override
            public void onSubscribe( Subscription subscription )
            {
                this.subscription = subscription;
                maybeRequest();
            }

            @Override
            public void onNext( Record record )
            {
                pendingRequested.decrementAndGet();
                enqueue( new RecordOrError( record, null ) );
            }

            @Override
            public void onError( Throwable throwable )
            {
                enqueue( new RecordOrError( null, throwable ) );
            }

            @Override
            public void onComplete()
            {
                enqueue( END );
            }

            void request( long numberOfRecords )
            {
                pendingRequested.addAndGet( numberOfRecords );
                subscription.request( numberOfRecords );
            }

            private void enqueue( RecordOrError recordOrError )
            {
                buffer.add( recordOrError );
                maybeProduce();
            }

            void close()
            {
                subscription.cancel();
            }
        }
    }

    private static class RecordOrError
    {
        private final Record record;
        private final Throwable error;

        RecordOrError( Record record, Throwable error )
        {
            this.record = record;
            this.error = error;
        }
    }
}
