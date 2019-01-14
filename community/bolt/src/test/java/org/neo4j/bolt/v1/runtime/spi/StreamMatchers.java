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
package org.neo4j.bolt.v1.runtime.spi;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NumberValue;

import static java.util.Arrays.asList;

public class StreamMatchers
{
    private StreamMatchers()
    {
    }

    public static Matcher<AnyValue> greaterThanOrEqualTo( long input )
    {
        return new TypeSafeMatcher<AnyValue>()
        {
            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Value = " + input );

            }

            @Override
            protected boolean matchesSafely( AnyValue value )
            {
                return value instanceof NumberValue && ((NumberValue) value).longValue() >= input;
            }
        };
    }

    public static Matcher<QueryResult.Record> eqRecord( final Matcher<?>... expectedFieldValues )
    {
        return new TypeSafeMatcher<QueryResult.Record>()
        {
            @Override
            protected boolean matchesSafely( QueryResult.Record item )
            {
                if ( expectedFieldValues.length != item.fields().length )
                {
                    return false;
                }

                for ( int i = 0; i < item.fields().length; i++ )
                {
                    if ( !expectedFieldValues[i].matches( item.fields()[i] ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Record[" )
                        .appendList( ", fields=[", ",", "]", asList( expectedFieldValues ) );

            }
        };
    }

}
