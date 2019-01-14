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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilteringNativeHitIteratorTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldFilterResults()
    {
        // given
        List<String> keys = new ArrayList<>();
        for ( int i = 0; i < 100; i++ )
        {
            // duplicates are fine
            keys.add( random.string() );
        }

        RawCursor<Hit<StringSchemaKey,NativeSchemaValue>,IOException> cursor = new ResultCursor( keys.iterator() );
        IndexQuery[] predicates = new IndexQuery[]{mock( IndexQuery.class )};
        Predicate<String> filter = string -> string.contains( "a" );
        when( predicates[0].acceptsValue( any( Value.class ) ) ).then( invocation -> filter.test( ((TextValue)invocation.getArgument( 0 )).stringValue() ) );
        FilteringNativeHitIterator<StringSchemaKey,NativeSchemaValue> iterator = new FilteringNativeHitIterator<>( cursor, new ArrayList<>(), predicates );
        List<Long> result = new ArrayList<>();

        // when
        while ( iterator.hasNext() )
        {
            result.add( iterator.next() );
        }

        // then
        for ( int i = 0; i < keys.size(); i++ )
        {
            if ( filter.test( keys.get( i ) ) )
            {
                assertTrue( result.remove( (long) i ) );
            }
        }
        assertTrue( result.isEmpty() );
    }
}
