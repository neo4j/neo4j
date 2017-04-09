/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.BoltIOException;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;

class CypherAdapterStream extends BoltResult
{
    private final Result delegate;
    private final String[] fieldNames;
    private CypherAdapterRecord currentRecord;
    private final Clock clock;

    CypherAdapterStream( Result delegate, Clock clock )
    {
        this.delegate = delegate;
        this.fieldNames = delegate.columns().toArray( new String[delegate.columns().size()] );
        this.currentRecord = new CypherAdapterRecord( fieldNames );
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
            visitor.visit( currentRecord.reset( row ) );
            return true;
        } );
        visitor.addMetadata( "result_consumed_after", clock.millis() - start );
        QueryExecutionType qt = delegate.getQueryExecutionType();
        visitor.addMetadata( "type", queryTypeCode( qt.queryType() ) );

        if ( delegate.getQueryStatistics().containsUpdates() )
        {
            Object stats = queryStats( delegate.getQueryStatistics() );
            visitor.addMetadata( "stats", stats );
        }
        if ( qt.requestedExecutionPlanDescription() )
        {
            ExecutionPlanDescription rootPlanTreeNode = delegate.getExecutionPlanDescription();
            String metadataFieldName = rootPlanTreeNode.hasProfilerStatistics() ? "profile" : "plan";
            visitor.addMetadata( metadataFieldName, ExecutionPlanConverter.convert( rootPlanTreeNode ) );
        }

        Iterable<Notification> notifications = delegate.getNotifications();
        if ( notifications.iterator().hasNext() )
        {
            visitor.addMetadata( "notifications", NotificationConverter.convert( notifications ) );
        }
    }

    private Map<String, Integer> queryStats( QueryStatistics queryStatistics )
    {
        Map<String, Integer> result = new HashMap<>();
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
        return result;
    }

    private void addIfNonZero( Map<String, Integer> map, String name, int count )
    {
        if ( count > 0 )
        {
            map.put( name, count );
        }
    }

    private String queryTypeCode( QueryExecutionType.QueryType queryType )
    {
        switch (queryType)
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

    private static class CypherAdapterRecord implements Record
    {
        private final Object[] fields; // This exists solely to avoid re-creating a new array for each record
        private final String[] fieldNames;

        private CypherAdapterRecord( String[] fieldNames )
        {
            this.fields = new Object[fieldNames.length];
            this.fieldNames = fieldNames;
        }

        @Override
        public Object[] fields()
        {
            return fields;
        }

        public CypherAdapterRecord reset( Result.ResultRow cypherRecord ) throws BoltIOException
        {
            for ( int i = 0; i < fields.length; i++ )
            {
                fields[i] = cypherRecord.get( fieldNames[i] );
            }
            return this;
        }
    }

    private static class NotificationConverter
    {
        public static Object convert( Iterable<Notification> notifications )
        {
            List<Object> out = new ArrayList<>();
            for ( Notification notification : notifications )
            {
                Map<String, Object> notificationMap = new HashMap<>( 4 );
                notificationMap.put( "code", notification.getCode() );
                notificationMap.put( "title", notification.getTitle() );
                notificationMap.put( "description", notification.getDescription() );
                notificationMap.put( "severity", notification.getSeverity().toString() );

                InputPosition pos = notification.getPosition(); // position is optional
                if ( !pos.equals( InputPosition.empty ) )
                {
                    // only add the position if it is not empty
                    Map<String,Object> posMap = new HashMap<>( 3 );
                    posMap.put( "offset", pos.getOffset() );
                    posMap.put( "line", pos.getLine() );
                    posMap.put( "column", pos.getColumn() );
                    notificationMap.put( "position", posMap );
                }

                out.add( notificationMap );
            }
            return out;
        }
    }
}
