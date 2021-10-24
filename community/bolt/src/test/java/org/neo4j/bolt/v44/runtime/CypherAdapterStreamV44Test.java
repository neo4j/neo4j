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
package org.neo4j.bolt.v44.runtime;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.Collections;

import org.neo4j.bolt.runtime.AbstractCypherAdapterStreamTest;
import org.neo4j.bolt.runtime.statemachine.impl.BoltAdapterSubscriber;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.values.virtual.MapValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.query;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

class CypherAdapterStreamV44Test extends AbstractCypherAdapterStreamTest
{

    @Test
    void shouldIncludeQueryStatisticContainsUpdates() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.containsUpdates() ).thenReturn( true );
        when( queryStatistics.getNodesCreated() ).thenReturn( 1 );
        when( queryStatistics.getNodesDeleted() ).thenReturn( 2 );
        when( queryStatistics.getRelationshipsCreated() ).thenReturn( 3 );
        when( queryStatistics.getRelationshipsDeleted() ).thenReturn( 4 );
        when( queryStatistics.getPropertiesSet() ).thenReturn( 5 );
        when( queryStatistics.getIndexesAdded() ).thenReturn( 6 );
        when( queryStatistics.getIndexesRemoved() ).thenReturn( 7 );
        when( queryStatistics.getConstraintsAdded() ).thenReturn( 8 );
        when( queryStatistics.getConstraintsRemoved() ).thenReturn( 9 );
        when( queryStatistics.getLabelsAdded() ).thenReturn( 10 );
        when( queryStatistics.getLabelsRemoved() ).thenReturn( 11 );

        QueryExecution result = mock( QueryExecution.class );
        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( query( READ_WRITE ) );
        subscriber.onResultCompleted( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );

        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L, 1337L );

        var stream = new TestAbstractCypherAdapterStream( result, subscriber, clock );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "type" ) ).isEqualTo( stringValue( "rw" ) );
        assertThat( meta.get( "stats" ) ).isEqualTo(
                mapValues( "nodes-created", intValue( 1 ), "nodes-deleted", intValue( 2 ), "relationships-created", intValue( 3 ), "relationships-deleted",
                           intValue( 4 ), "properties-set", intValue( 5 ), "indexes-added", intValue( 6 ), "indexes-removed", intValue( 7 ),
                           "constraints-added", intValue( 8 ), "constraints-removed", intValue( 9 ), "labels-added", intValue( 10 ),
                           "labels-removed", intValue( 11 ), "contains-updates", booleanValue( true ) ) );
    }

    @Test
    void shouldIncludeQueryStatisticContainsSystemUpdates() throws Throwable
    {
        // Given
        QueryStatistics queryStatistics = mock( QueryStatistics.class );
        when( queryStatistics.getSystemUpdates() ).thenReturn( 1 );
        when( queryStatistics.containsUpdates() ).thenReturn( false );
        when( queryStatistics.containsSystemUpdates() ).thenReturn( true );

        QueryExecution result = mock( QueryExecution.class );
        BoltAdapterSubscriber subscriber = new BoltAdapterSubscriber();
        when( result.fieldNames() ).thenReturn( new String[0] );
        when( result.executionType() ).thenReturn( query( READ_WRITE ) );
        subscriber.onResultCompleted( queryStatistics );
        when( result.getNotifications() ).thenReturn( Collections.emptyList() );

        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 0L, 1337L );

        var stream = new TestAbstractCypherAdapterStream( result, subscriber, clock );

        // When
        MapValue meta = metadataOf( stream );

        // Then
        assertThat( meta.get( "type" ) ).isEqualTo( stringValue( "rw" ) );
        assertThat( meta.get( "stats" ) ).isEqualTo(
                mapValues( "system-updates", intValue( 1 ), "contains-system-updates", booleanValue( true ) ) );
    }

    private static class TestAbstractCypherAdapterStream extends CypherAdapterStreamV44
    {
        TestAbstractCypherAdapterStream( QueryExecution queryExecution, BoltAdapterSubscriber querySubscriber, Clock clock )
        {
            super( queryExecution, querySubscriber, clock, "randomdb" );
        }

        @Override
        protected void addDatabaseName( RecordConsumer recordConsumer )
        {
        }

        @Override
        protected void addRecordStreamingTime( long time, RecordConsumer recordConsumer )
        {
        }
    }

}
