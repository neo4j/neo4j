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
package org.neo4j.fabric.bolt;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.executor.Exceptions;
import org.neo4j.fabric.stream.FabricExecutionStatementResult;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Rx2SyncStream;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;

public class BoltQueryExecutionImpl implements BoltQueryExecution
{
    private final QueryExecutionImpl queryExecution;

    public BoltQueryExecutionImpl( FabricExecutionStatementResult statementResult, QuerySubscriber subscriber, FabricConfig fabricConfig )
    {
        var config = fabricConfig.getDataStream();
        var rx2SyncStream = new Rx2SyncStream( statementResult.records(), config.getBatchSize() );
        queryExecution =
                new QueryExecutionImpl( rx2SyncStream, subscriber, statementResult.columns(), statementResult.summary(), statementResult.queryExecutionType() );
    }

    @Override
    public QueryExecution getQueryExecution()
    {
        return queryExecution;
    }

    @Override
    public void close()
    {
        queryExecution.cancel();
    }

    @Override
    public void terminate()
    {
        queryExecution.cancel();
    }

    private static class QueryExecutionImpl implements QueryExecution
    {

        private final Rx2SyncStream rx2SyncStream;
        private final QuerySubscriber subscriber;
        private boolean hasMore = true;
        private boolean initialised;
        private final Mono<Summary> summary;
        private final Mono<QueryExecutionType> queryExecutionType;
        private final Supplier<List<String>> columns;

        private QueryExecutionImpl( Rx2SyncStream rx2SyncStream, QuerySubscriber subscriber, Flux<String> columns, Mono<Summary> summary,
                Mono<QueryExecutionType> queryExecutionType )
        {
            this.rx2SyncStream = rx2SyncStream;
            this.subscriber = subscriber;
            this.summary = summary;
            this.queryExecutionType = queryExecutionType;

            AtomicReference<List<String>> columnsStore = new AtomicReference<>();
            this.columns = () ->
            {
                if ( columnsStore.get() == null )
                {
                    columnsStore.compareAndSet( null, columns.collectList().block() );
                }

                return columnsStore.get();
            };
        }

        private Summary getSummary()
        {
            return summary.cache().block();
        }

        @Override
        public QueryExecutionType executionType()
        {
            return queryExecutionType.cache().block();
        }

        @Override
        public ExecutionPlanDescription executionPlanDescription()
        {
            return getSummary().executionPlanDescription();
        }

        @Override
        public Iterable<Notification> getNotifications()
        {
            return getSummary().getNotifications();
        }

        @Override
        public String[] fieldNames()
        {
            return columns.get().toArray( new String[0] );
        }

        @Override
        public void request( long numberOfRecords ) throws Exception
        {
            if ( !hasMore )
            {
                return;
            }

            if ( !initialised )
            {
                initialised = true;
                subscriber.onResult( columns.get().size() );
            }

            try
            {
                for ( int i = 0; i < numberOfRecords; i++ )
                {
                    Record record = rx2SyncStream.readRecord();

                    if ( record == null )
                    {
                        hasMore = false;
                        subscriber.onResultCompleted( getSummary().getQueryStatistics() );
                        return;
                    }

                    subscriber.onRecord();
                    publishFields( record );
                    subscriber.onRecordCompleted();
                }
            }
            catch ( Exception e )
            {
                throw Exceptions.transform(Status.Statement.ExecutionFailed, e);
            }
        }

        private void publishFields( Record record ) throws Exception
        {
            for ( int i = 0; i < columns.get().size(); i++ )
            {
                subscriber.onField( i, record.getValue( i ) );
            }
        }

        @Override
        public void cancel()
        {
            rx2SyncStream.close();
        }

        @Override
        public boolean await()
        {
            return hasMore;
        }

        @Override
        public boolean isVisitable()
        {
            return false;
        }

        @Override
        public <VisitationException extends Exception> QueryStatistics accept( Result.ResultVisitor<VisitationException> visitor )
        {
            throw new IllegalStateException( "Results are not visitable" );
        }
    }
}
