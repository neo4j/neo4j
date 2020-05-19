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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.LocalExecutionSummary;
import org.neo4j.fabric.stream.summary.EmptySummary;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

public final class StatementResults
{
    private StatementResults()
    {
    }

    public static StatementResult map( StatementResult statementResult, UnaryOperator<Flux<Record>> func )
    {
        return new BasicStatementResult( statementResult.columns(),
                                         func.apply( statementResult.records() ),
                                         statementResult.summary(),
                                         statementResult.executionType() );
    }

    public static StatementResult initial()
    {
        return new BasicStatementResult( Flux.empty(), Flux.just( Records.empty() ), Mono.empty(), Mono.empty() );
    }

    public static StatementResult create( Function<QuerySubscriber,QueryExecution> execution )
    {
        try
        {
            QuerySubject querySubject = new QuerySubject();
            QueryExecution queryExecution = execution.apply( querySubject );
            querySubject.setQueryExecution( queryExecution );
            return create(
                    Flux.fromArray( queryExecution.fieldNames() ),
                    Flux.from( querySubject ),
                    querySubject.getSummary(),
                    Mono.just( queryExecution.executionType() )
            );
        }
        catch ( RuntimeException re )
        {
            return error( re );
        }
    }

    public static StatementResult create( Flux<String> columns, Flux<Record> records, Mono<Summary> summary, Mono<QueryExecutionType> executionType )
    {
        return new BasicStatementResult( columns, records, summary, executionType );
    }

    public static <E extends Throwable> StatementResult withErrorMapping( StatementResult statementResult, Class<E> type,
            Function<? super E,? extends Throwable> mapper )
    {
        var columns = statementResult.columns().onErrorMap( type, mapper );
        var records = statementResult.records().onErrorMap( type, mapper );
        var summary = statementResult.summary().onErrorMap( type, mapper );
        var executionType = statementResult.executionType().onErrorMap( type, mapper );

        return create( columns, records, summary, executionType );
    }

    public static StatementResult error( Throwable err )
    {
        return new BasicStatementResult( Flux.error( err ), Flux.error( err ), Mono.error( err ), Mono.error( err ) );
    }

    public static StatementResult trace( StatementResult input )
    {
        return new BasicStatementResult(
                input.columns(),
                input.records().doOnEach( signal ->
                {
                    if ( signal.hasValue() )
                    {
                        System.out.println( String.join( ", ", signal.getType().toString(), Records.show( signal.get() ) ) );
                    }
                    else if ( signal.hasError() )
                    {
                        System.out.println( String.join( ", ", signal.getType().toString(), signal.getThrowable().toString() ) );
                    }
                    else
                    {
                        System.out.println( String.join( ", ", signal.getType().toString() ) );
                    }
                } ),
                input.summary(),
                input.executionType() );
    }

    private static class BasicStatementResult implements StatementResult
    {
        private final Flux<String> columns;
        private final Flux<Record> records;
        private final Mono<Summary> summary;
        private final Mono<QueryExecutionType> executionType;

        BasicStatementResult( Flux<String> columns, Flux<Record> records, Mono<Summary> summary,
                              Mono<QueryExecutionType> executionType )
        {
            this.columns = columns;
            this.records = records;
            this.summary = summary;
            this.executionType = executionType;
        }

        @Override
        public Flux<String> columns()
        {
            return columns;
        }

        @Override
        public Flux<Record> records()
        {
            return records;
        }

        @Override
        public Mono<Summary> summary()
        {
            return summary;
        }

        @Override
        public Mono<QueryExecutionType> executionType()
        {
            return executionType;
        }
    }

    private static class QuerySubject extends RecordQuerySubscriber implements Publisher<Record>
    {
        private final CompletableFuture<Summary> summaryFuture = new CompletableFuture<>();

        private Subscriber<? super Record> subscriber;
        private QueryExecution queryExecution;
        private QueryStatistics statistics;
        private Throwable cachedError;
        private boolean cachedCompleted;
        private boolean errorReceived;

        void setQueryExecution( QueryExecution queryExecution )
        {
            this.queryExecution = queryExecution;
        }

        Mono<Summary> getSummary()
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
                            queryExecution.await();
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

    private abstract static class RecordQuerySubscriber implements QuerySubscriber
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
}
