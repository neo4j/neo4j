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

import java.util.concurrent.Executor;

public class CompletionDelegatingOperator extends FluxOperator<Record,Record>
{
    private final Flux<Record> recordStream;
    private final Executor executor;

    public CompletionDelegatingOperator( Flux<Record> recordStream, Executor executor )
    {
        super( recordStream );
        this.recordStream = recordStream;
        this.executor = executor;
    }

    @Override
    public void subscribe( CoreSubscriber downstreamSubscriber )
    {
        recordStream.subscribeWith( new UpstreamSubscriber( downstreamSubscriber ) );
    }

    private class UpstreamSubscriber implements Subscriber<Record>
    {

        private final Subscriber<Record> downstreamSubscriber;

        UpstreamSubscriber( Subscriber<Record> downstreamSubscriber )
        {
            this.downstreamSubscriber = downstreamSubscriber;
        }

        @Override
        public void onSubscribe( Subscription subscription )
        {
            downstreamSubscriber.onSubscribe( subscription );
        }

        @Override
        public void onNext( Record record )
        {
            downstreamSubscriber.onNext( record );
        }

        @Override
        public void onError( Throwable throwable )
        {
            executor.execute( () -> downstreamSubscriber.onError( throwable ) );
        }

        @Override
        public void onComplete()
        {
            executor.execute( downstreamSubscriber::onComplete );
        }
    }
}
