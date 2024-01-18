/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.runtime;

import java.time.Clock;
import java.util.Arrays;

import org.neo4j.bolt.runtime.statemachine.impl.BoltAdapterSubscriber;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.util.Preconditions;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.messaging.AbstractStreamingMessage.STREAM_LIMIT_UNLIMITED;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.utf8Value;

public abstract class AbstractCypherAdapterStream implements BoltResult
{
    private static final String TYPE = "type";
    private static final String STATS = "stats";
    private static final String PROFILE = "profile";
    private static final String PLAN = "plan";
    private static final String NOTIFICATIONS = "notifications";
    private static final TextValue READ_ONLY = Values.utf8Value( new byte[]{'r'} );
    private static final TextValue READ_WRITE = Values.utf8Value( new byte[]{'r', 'w'} );
    private static final TextValue WRITE = Values.utf8Value( new byte[]{'w'} );
    private static final TextValue SCHEMA_WRITE = Values.utf8Value( new byte[]{'s'} );

    private final QueryExecution queryExecution;
    private final String[] fieldNames;
    protected final Clock clock;
    private final BoltAdapterSubscriber querySubscriber;
    private long timeSpentStreaming;

    private static final Long STREAM_UNLIMITED_BATCH_SIZE = Long.MAX_VALUE;

    public AbstractCypherAdapterStream( QueryExecution queryExecution,
            BoltAdapterSubscriber querySubscriber, Clock clock )
    {
        this.queryExecution = queryExecution;
        this.fieldNames = queryExecution.fieldNames();
        this.querySubscriber = querySubscriber;
        this.clock = clock;
    }

    @Override
    public void close()
    {
        queryExecution.cancel();
    }

    @Override
    public String[] fieldNames()
    {
        return fieldNames;
    }

    @Override
    public boolean handleRecords( RecordConsumer recordConsumer, long size ) throws Throwable
    {
        long start = clock.millis();
        this.querySubscriber.setRecordConsumer( recordConsumer );

        boolean hasMore = true;
        if ( size == STREAM_LIMIT_UNLIMITED )
        {
            while ( hasMore )
            {
                // Continuously pull until the whole stream is done
                queryExecution.request( STREAM_UNLIMITED_BATCH_SIZE );
                hasMore = queryExecution.await();
            }
        }
        else
        {
            queryExecution.request( size );
            hasMore = queryExecution.await();
        }

        querySubscriber.assertSucceeded();
        timeSpentStreaming += clock.millis() - start;
        if ( !hasMore )
        {
            addRecordStreamingTime( timeSpentStreaming , recordConsumer );
            addDatabaseName( recordConsumer );
            addMetadata( querySubscriber.queryStatistics(), recordConsumer );
        }
        return hasMore;
    }

    @Override
    public boolean discardRecords( DiscardingRecordConsumer consumer, long size ) throws Throwable
    {
        Preconditions.checkArgument( size == STREAM_LIMIT_UNLIMITED,
                                     "Currently it is only supported to discard ALL records, but it was requested to discard " + size );

        if ( queryExecution.executionType().queryType() == QueryExecutionType.QueryType.READ_ONLY )
        {
            long start = clock.millis();
            queryExecution.cancel();
            queryExecution.await();
            addRecordStreamingTime( clock.millis() - start, consumer );
            // The subscriber didn't get statistics since the query did not finish execution, but
            // for read queries we know that empty statistics are correct.
            addMetadata( QueryStatistics.EMPTY, consumer );
            return false;
        }
        else
        {
            // For READ-WRITE or WRITE queries, we need to continue execution but do not need to send records any longer
            return handleRecords( consumer, size );
        }
    }

    protected abstract void addDatabaseName( RecordConsumer recordConsumer );

    protected abstract void addRecordStreamingTime( long time, RecordConsumer recordConsumer );

    private void addMetadata( QueryStatistics statistics, RecordConsumer recordConsumer )
    {
        QueryExecutionType qt = queryExecution.executionType();
        recordConsumer.addMetadata( TYPE, queryTypeCode( qt.queryType() ) );

        addQueryStatistics( statistics, recordConsumer );

        if ( qt.requestedExecutionPlanDescription() )
        {
            ExecutionPlanDescription rootPlanTreeNode = queryExecution.executionPlanDescription();
            String metadataFieldName = rootPlanTreeNode.hasProfilerStatistics() ? PROFILE : PLAN;
            recordConsumer.addMetadata( metadataFieldName, ExecutionPlanConverter.convert( rootPlanTreeNode ) );
        }

        Iterable<Notification> notifications = queryExecution.getNotifications();
        if ( notifications.iterator().hasNext() )
        {
            recordConsumer.addMetadata( NOTIFICATIONS, convertNotifications( notifications ) );
        }
    }

    private void addQueryStatistics( QueryStatistics statistics, RecordConsumer recordConsumer )
    {
        if ( statistics.containsUpdates() )
        {
            MapValue stats = queryStats( statistics ).build();
            recordConsumer.addMetadata( STATS, stats );
        }
        else if ( statistics.containsSystemUpdates() )
        {
            MapValue stats = systemQueryStats( statistics ).build();
            recordConsumer.addMetadata( STATS, stats );
        }
    }

    @Override
    public String toString()
    {
        return "CypherAdapterStream{" + "delegate=" + queryExecution + ", fieldNames=" + Arrays.toString( fieldNames ) +
               '}';
    }

    protected MapValueBuilder queryStats( QueryStatistics queryStatistics )
    {
        MapValueBuilder builder = new MapValueBuilder();
        addIfNonZero( builder, "nodes-created", queryStatistics.getNodesCreated() );
        addIfNonZero( builder, "nodes-deleted", queryStatistics.getNodesDeleted() );
        addIfNonZero( builder, "relationships-created", queryStatistics.getRelationshipsCreated() );
        addIfNonZero( builder, "relationships-deleted", queryStatistics.getRelationshipsDeleted() );
        addIfNonZero( builder, "properties-set", queryStatistics.getPropertiesSet() );
        addIfNonZero( builder, "labels-added", queryStatistics.getLabelsAdded() );
        addIfNonZero( builder, "labels-removed", queryStatistics.getLabelsRemoved() );
        addIfNonZero( builder, "indexes-added", queryStatistics.getIndexesAdded() );
        addIfNonZero( builder, "indexes-removed", queryStatistics.getIndexesRemoved() );
        addIfNonZero( builder, "constraints-added", queryStatistics.getConstraintsAdded() );
        addIfNonZero( builder, "constraints-removed", queryStatistics.getConstraintsRemoved() );
        return builder;
    }

    protected MapValueBuilder systemQueryStats( QueryStatistics queryStatistics )
    {
        MapValueBuilder builder = new MapValueBuilder();
        addIfNonZero( builder, "system-updates", queryStatistics.getSystemUpdates() );
        return builder;
    }

    protected static void addIfNonZero( MapValueBuilder builder, String name, int count )
    {
        if ( count > 0 )
        {
            builder.add( name, intValue( count ) );
        }
    }

    protected static void addIfTrue( MapValueBuilder builder, String name, boolean value )
    {
        if ( value )
        {
            builder.add( name, booleanValue( true ) );
        }
    }

    private static TextValue queryTypeCode( QueryExecutionType.QueryType queryType )
    {
        switch ( queryType )
        {
        case READ_ONLY:
            return READ_ONLY;

        case READ_WRITE:
            return READ_WRITE;

        case WRITE:
            return WRITE;

        case SCHEMA_WRITE:
        case DBMS://TODO: Dear reviewer, what about this
            return SCHEMA_WRITE;

        default:
            throw new IllegalStateException( format( "%s is not a known query type", queryType ) );
        }
    }

    private static AnyValue convertNotifications( Iterable<Notification> notifications )
    {
        ListValueBuilder listValueBuilder = ListValueBuilder.newListBuilder();
        for ( Notification notification : notifications )
        {
            InputPosition pos = notification.getPosition(); // position is optional
            boolean includePosition = !pos.equals( InputPosition.empty );
            int size = includePosition ? 5 : 4;
            MapValueBuilder builder = new MapValueBuilder( size );

            builder.add( "code", utf8Value( notification.getCode() ) );
            builder.add( "title", utf8Value( notification.getTitle() ) );
            builder.add( "description", utf8Value( notification.getDescription() ) );
            builder.add( "severity", utf8Value( notification.getSeverity().toString() ) );

            if ( includePosition )
            {
                // only add the position if it is not empty
                builder.add( "position", VirtualValues.map( new String[]{"offset", "line", "column"},
                        new AnyValue[]{
                                intValue( pos.getOffset() ),
                                intValue( pos.getLine() ),
                                intValue( pos.getColumn() )} ) );
            }

            listValueBuilder.add( builder.build() );
        }
        return listValueBuilder.build();
    }
}
