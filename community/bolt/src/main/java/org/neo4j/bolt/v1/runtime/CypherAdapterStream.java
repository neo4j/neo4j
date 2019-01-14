/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

public class CypherAdapterStream implements BoltResult
{
    private final QueryResult delegate;
    private final String[] fieldNames;
    private final Clock clock;

    public CypherAdapterStream( QueryResult delegate, Clock clock )
    {
        this.delegate = delegate;
        this.fieldNames = delegate.fieldNames();
        this.clock = clock;
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public String[] fieldNames()
    {
        return fieldNames;
    }

    @Override
    public void accept( final Visitor visitor ) throws Exception
    {
        long start = clock.millis();
        delegate.accept( row ->
        {
            visitor.visit( row );
            return true;
        } );
        addRecordStreamingTime( visitor, clock.millis() - start );
        QueryExecutionType qt = delegate.executionType();
        visitor.addMetadata( "type", Values.stringValue( queryTypeCode( qt.queryType() ) ) );

        if ( delegate.queryStatistics().containsUpdates() )
        {
            MapValue stats = queryStats( delegate.queryStatistics() );
            visitor.addMetadata( "stats", stats );
        }
        if ( qt.requestedExecutionPlanDescription() )
        {
            ExecutionPlanDescription rootPlanTreeNode = delegate.executionPlanDescription();
            String metadataFieldName = rootPlanTreeNode.hasProfilerStatistics() ? "profile" : "plan";
            visitor.addMetadata( metadataFieldName, ExecutionPlanConverter.convert( rootPlanTreeNode ) );
        }

        Iterable<Notification> notifications = delegate.getNotifications();
        if ( notifications.iterator().hasNext() )
        {
            visitor.addMetadata( "notifications", NotificationConverter.convert( notifications ) );
        }
    }

    protected void addRecordStreamingTime( Visitor visitor, long time )
    {
        visitor.addMetadata( "result_consumed_after", longValue( time ) );
    }

    @Override
    public String toString()
    {
        return "CypherAdapterStream{" + "delegate=" + delegate + ", fieldNames=" + Arrays.toString( fieldNames ) + '}';
    }

    private MapValue queryStats( QueryStatistics queryStatistics )
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
        return builder.build();
    }

    private void addIfNonZero( MapValueBuilder builder, String name, int count )
    {
        if ( count > 0 )
        {
            builder.add( name, intValue( count ) );
        }
    }

    private String queryTypeCode( QueryExecutionType.QueryType queryType )
    {
        switch ( queryType )
        {
        case READ_ONLY:
            return "r";

        case READ_WRITE:
            return "rw";

        case WRITE:
            return "w";

        case SCHEMA_WRITE:
            return "s";

        default:
            return queryType.name();
        }
    }

    private static class NotificationConverter
    {
        public static AnyValue convert( Iterable<Notification> notifications )
        {
            List<AnyValue> out = new ArrayList<>();
            for ( Notification notification : notifications )
            {
                InputPosition pos = notification.getPosition(); // position is optional
                boolean includePosition = !pos.equals( InputPosition.empty );
                int size = includePosition ? 5 : 4;
                MapValueBuilder builder = new MapValueBuilder( size );

                builder.add( "code", stringValue( notification.getCode() ) );
                builder.add( "title", stringValue( notification.getTitle() ) );
                builder.add( "description", stringValue( notification.getDescription() ) );
                builder.add( "severity", stringValue( notification.getSeverity().toString() ) );

                if ( includePosition )
                {
                    // only add the position if it is not empty
                    builder.add( "position", VirtualValues.map( new String[]{"offset", "line", "column"},
                            new AnyValue[]{
                                    intValue( pos.getOffset() ),
                                    intValue( pos.getLine() ),
                                    intValue( pos.getColumn() )  } ) );
                }

                out.add( builder.build() );
            }
            return VirtualValues.fromList( out );
        }
    }
}
