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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.LocalExecutionSummary;
import org.neo4j.fabric.stream.summary.EmptySummary;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

public interface QuerySubject extends QuerySubscriber, Publisher<Record>
{
    void setQueryExecution( QueryExecution queryExecution );

    Mono<Summary> getSummary();

    class BasicQuerySubject extends RecordQuerySubscriber implements QuerySubject
    {
        private final CompletableFuture<Summary> summaryFuture = new CompletableFuture<>();

        private Subscriber<? super Record> subscriber;
        private QueryExecution queryExecution;
        private QueryStatistics statistics;
        private Throwable cachedError;
        private boolean cachedCompleted;
        private boolean errorReceived;

        public void setQueryExecution( QueryExecution queryExecution )
        {
            this.queryExecution = queryExecution;
        }

        public Mono<Summary> getSummary()
        {
            return Mono.fromFuture( summaryFuture );
        }

        @Override
        public void onNext( Record record )
        {
            subscriber.onNext( record );
        }

        @Override
        public void onError( Throwable throwable )
        {
            errorReceived = true;

            if ( subscriber == null )
            {
                cachedError = throwable;
            }
            else
            {
                subscriber.onError( throwable );
            }

            summaryFuture.completeExceptionally( throwable );
        }

        @Override
        public void onResultCompleted( QueryStatistics statistics )
        {
            this.statistics = statistics;
            if ( subscriber == null )
            {
                cachedCompleted = true;
            }
            else
            {
                subscriber.onComplete();
                completeSummary();
            }
        }

        private void completeSummary()
        {
            summaryFuture.complete( new LocalExecutionSummary( queryExecution, statistics ) );
        }

        @Override
        public void subscribe( Subscriber<? super Record> subscriber )
        {

            if ( this.subscriber != null )
            {
                throw new FabricException( Status.General.UnknownError, "Already subscribed" );
            }
            this.subscriber = subscriber;
            Subscription subscription = new Subscription()
            {

                private final Object requestLock = new Object();
                private long pendingRequests;
                // a flag indicating if there is a thread requesting from upstream
                private boolean producing;

                @Override
                public void request( long size )
                {
                    synchronized ( requestLock )
                    {
                        pendingRequests += size;
                        // check if another thread is already requesting
                        if ( producing )
                        {
                            return;
                        }

                        producing = true;
                    }

                    try
                    {
                        while ( true )
                        {
                            long toRequest;
                            synchronized ( requestLock )
                            {
                                toRequest = pendingRequests;
                                if ( toRequest == 0 )
                                {
                                    return;
                                }

                                pendingRequests = 0;
                            }

                            doRequest( toRequest );
                        }
                    }
                    finally
                    {
                        synchronized ( requestLock )
                        {
                            producing = false;
                        }
                    }
                }

                private void doRequest( long size )
                {
                    maybeSendCachedEvents();
                    try
                    {
                        queryExecution.request( size );

                        // If 'await' is called after an error has been received, it will throw with the same error.
                        // Reactor operators don't like when 'onError' is called more than once. Typically, the second call throws an exception,
                        // which can have a disastrous effect on the RX pipeline
                        if ( !errorReceived )
                        {
                            var hasMore = queryExecution.await();
                            // Workaround for some queryExecution:s where there are no results but onResultCompleted is never called.
                            if ( !hasMore )
                            {
                                cachedCompleted = true;
                                maybeSendCachedEvents();
                            }
                        }
                    }
                    catch ( Exception e )
                    {
                        subscriber.onError( e );
                    }
                }

                @Override
                public void cancel()
                {
                    try
                    {
                        queryExecution.cancel();
                    }
                    catch ( Throwable e )
                    {
                        // ignore
                    }

                    if ( !summaryFuture.isDone() )
                    {
                        summaryFuture.complete( new EmptySummary() );
                    }
                }
            };
            subscriber.onSubscribe( subscription );
            maybeSendCachedEvents();
        }

        private void maybeSendCachedEvents()
        {
            if ( cachedError != null )
            {
                subscriber.onError( cachedError );
                cachedError = null;
            }
            else if ( cachedCompleted )
            {
                subscriber.onComplete();
                cachedCompleted = false;
                completeSummary();
            }
        }
    }

    abstract class RecordQuerySubscriber implements QuerySubscriber
    {
        private int numberOfFields;
        private AnyValue[] fields;

        @Override
        public void onResult( int numberOfFields )
        {
            this.numberOfFields = numberOfFields;
        }

        @Override
        public void onRecord()
        {
            fields = new AnyValue[numberOfFields];
        }

        @Override
        public void onField( int offset, AnyValue value )
        {
            fields[offset] = value;
        }

        @Override
        public void onRecordCompleted()
        {
            onNext( Records.of( fields ) );
        }

        abstract void onNext( Record record );
    }

    class TaggingQuerySubject extends BasicQuerySubject implements QuerySubject
    {
        private final long sourceTag;

        public TaggingQuerySubject( long sourceId )
        {
            this.sourceTag = SourceTagging.makeSourceTag( sourceId );
        }

        @Override
        public void onField( int offset, AnyValue value )
        {
            AnyValue tagged = withTaggedId( value );
            super.onField( offset, tagged );
        }

        private AnyValue withTaggedId( AnyValue value )
        {
            if ( value instanceof VirtualNodeValue )
            {
                if ( value instanceof NodeValue )
                {
                    return withTaggedId( (NodeValue) value );
                }
                else
                {
                    throw unableToTagError( value );
                }
            }
            else if ( value instanceof VirtualRelationshipValue )
            {
                if ( value instanceof RelationshipValue )
                {
                    return withTaggedId( (RelationshipValue) value );
                }
                else
                {
                    throw unableToTagError( value );
                }
            }
            else if ( value instanceof PathValue )
            {
                return withTaggedId( (PathValue) value );
            }
            else if ( value instanceof ListValue )
            {
                return withTaggedId( (ListValue) value );
            }
            else if ( value instanceof MapValue )
            {
                return withTaggedId( (MapValue) value );
            }
            else
            {
                return value;
            }
        }

        private NodeValue withTaggedId( NodeValue nodeValue )
        {
            return VirtualValues.nodeValue(
                    tag( nodeValue.id() ),
                    nodeValue.labels(),
                    nodeValue.properties() );
        }

        private RelationshipValue withTaggedId( RelationshipValue relationshipValue )
        {
            return VirtualValues.relationshipValue(
                    tag( relationshipValue.id() ),
                    withTaggedId( relationshipValue.startNode() ),
                    withTaggedId( relationshipValue.endNode() ),
                    relationshipValue.type(),
                    relationshipValue.properties() );
        }

        private PathValue withTaggedId( PathValue pathValue )
        {
            return VirtualValues.path(
                    Arrays.stream( pathValue.nodes() ).map( this::withTaggedId ).toArray( NodeValue[]::new ),
                    Arrays.stream( pathValue.relationships() ).map( this::withTaggedId ).toArray( RelationshipValue[]::new )
            );
        }

        private ListValue withTaggedId( ListValue listValue )
        {
            return VirtualValues.list(
                    Arrays.stream( listValue.asArray() ).map( this::withTaggedId ).toArray( AnyValue[]::new ) );
        }

        private MapValue withTaggedId( MapValue mapValue )
        {
            MapValueBuilder builder = new MapValueBuilder( mapValue.size() );
            mapValue.foreach( ( key, value ) -> builder.add( key, withTaggedId( value ) ) );
            return builder.build();
        }

        private long tag( long id )
        {
            return SourceTagging.tagId( id, sourceTag );
        }

        private FabricException unableToTagError( AnyValue value )
        {
            return new FabricException( Status.General.UnknownError, "Unable to add source tag to entity of type " + value.getTypeName() );
        }
    }
}
