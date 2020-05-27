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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;

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

    public static StatementResult connectVia( SubscribableExecution execution, QuerySubject subject )
    {
        QueryExecution queryExecution = execution.subscribe( subject );
        subject.setQueryExecution( queryExecution );
        return create(
                Flux.fromArray( queryExecution.fieldNames() ),
                Flux.from( subject ),
                subject.getSummary(),
                Mono.just( queryExecution.executionType() )
        );
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

    @FunctionalInterface
    public interface SubscribableExecution
    {
        QueryExecution subscribe( QuerySubscriber subscriber );
    }
}
