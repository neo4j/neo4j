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
package org.neo4j.cypher.internal.javacompat;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriberAdapter;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EagerResultTest
{

    @Test
    void shouldStreamResultToSubscriber() throws Exception
    {
        var originalResult = mock( Result.class );
        when( originalResult.hasNext() ).thenReturn( true, true, true, false );
        //noinspection unchecked
        when( originalResult.next() ).thenReturn( Collections.singletonMap( "foo", "bar" ),
                                                  Collections.singletonMap( "foo", "bar2" ),
                                                  Collections.singletonMap( "foo", "bar3" ) );

        var innerExecution = mock( QueryExecution.class );
        when( innerExecution.fieldNames() ).thenReturn( new String[]{"foo"} );

        var eagerResult = new EagerResult( originalResult, null );
        var subscriber = new LastSeenSubscriber();
        eagerResult.consume();

        // when
        var execution = eagerResult.streamToSubscriber( subscriber, innerExecution );
        // then
        assertEquals( subscriber.getLastSeen(), Collections.EMPTY_LIST );

        // when
        execution.request( 1 );
        execution.await();
        // then
        assertEquals( subscriber.getLastSeen(), Collections.singletonList( Values.stringValue( "bar" ) ) );

        // when
        execution.request( Long.MAX_VALUE );
        execution.await();

        // then
        assertEquals( subscriber.getLastSeen(), Arrays.asList( Values.stringValue( "bar" ),
                                                               Values.stringValue( "bar2" ),
                                                               Values.stringValue( "bar3" ) ) );
    }

    private static class LastSeenSubscriber extends QuerySubscriberAdapter
    {
        private List<AnyValue> lastSeen = new ArrayList<>();

        @Override
        public void onField( AnyValue value )
        {
            lastSeen.add( value );
        }

        public List<AnyValue> getLastSeen()
        {
            return lastSeen;
        }
    }
}
