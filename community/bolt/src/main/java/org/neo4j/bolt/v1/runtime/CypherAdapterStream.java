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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.BoltIOException;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

class CypherAdapterStream extends BoltResult
{
    private final QueryResult delegate;
    private final String[] fieldNames;
    private final Clock clock;

    CypherAdapterStream( QueryResult delegate, Clock clock )
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
        visitor.addMetadata( "result_consumed_after", longValue( clock.millis() - start ) );
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

    private MapValue queryStats( QueryStatistics queryStatistics )
    {
        Map<String,AnyValue> result = new HashMap<>();
        addIfNonZero( result, "nodes-created", queryStatistics.getNodesCreated() );
        addIfNonZero( result, "nodes-deleted", queryStatistics.getNodesDeleted() );
        addIfNonZero( result, "relationships-created", queryStatistics.getRelationshipsCreated() );
        addIfNonZero( result, "relationships-deleted", queryStatistics.getRelationshipsDeleted() );
        addIfNonZero( result, "properties-set", queryStatistics.getPropertiesSet() );
        addIfNonZero( result, "labels-added", queryStatistics.getLabelsAdded() );
        addIfNonZero( result, "labels-removed", queryStatistics.getLabelsRemoved() );
        addIfNonZero( result, "indexes-added", queryStatistics.getIndexesAdded() );
        addIfNonZero( result, "indexes-removed", queryStatistics.getIndexesRemoved() );
        addIfNonZero( result, "constraints-added", queryStatistics.getConstraintsAdded() );
        addIfNonZero( result, "constraints-removed", queryStatistics.getConstraintsRemoved() );
        return VirtualValues.map( result );
    }

    private void addIfNonZero( Map<String,AnyValue> map, String name, int count )
    {
        if ( count > 0 )
        {
            map.put( name, intValue( count ) );
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
                Map<String,AnyValue> notificationMap = new HashMap<>( 4 );
                notificationMap.put( "code", stringValue( notification.getCode() ) );
                notificationMap.put( "title", stringValue( notification.getTitle() ) );
                notificationMap.put( "description", stringValue( notification.getDescription() ) );
                notificationMap.put( "severity", stringValue( notification.getSeverity().toString() ) );

                InputPosition pos = notification.getPosition(); // position is optional
                if ( !pos.equals( InputPosition.empty ) )
                {
                    // only add the position if it is not empty
                    Map<String,AnyValue> posMap = new HashMap<>( 3 );
                    posMap.put( "offset", intValue( pos.getOffset() ) );
                    posMap.put( "line", intValue( pos.getLine() ) );
                    posMap.put( "column", intValue( pos.getColumn() ) );
                    notificationMap.put( "position", VirtualValues.map( posMap ) );
                }

                out.add( VirtualValues.map( notificationMap ) );
            }
            return VirtualValues.fromList( out );
        }
    }
}
