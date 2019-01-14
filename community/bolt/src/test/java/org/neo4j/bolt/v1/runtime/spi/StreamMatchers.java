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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public static Matcher<BoltResult> equalsStream( final String[] fieldNames, final Matcher... records )
    {
        return new TypeSafeMatcher<BoltResult>()
        {
            @Override
            protected boolean matchesSafely( BoltResult item )
            {
                if ( !Arrays.equals( fieldNames, item.fieldNames() ) )
                {
                    return false;
                }
                final Iterator<Matcher> expected = asList( records ).iterator();
                final AtomicBoolean matched = new AtomicBoolean( true );
                try
                {
                    item.accept( new BoltResult.Visitor()
                    {
                        @Override
                        public void visit( QueryResult.Record record )
                        {
                            if ( !expected.hasNext() || !expected.next().matches( record ) )
                            {
                                matched.set( false );
                            }
                        }

                        @Override
                        public void addMetadata( String key, AnyValue value )
                        {

                        }
                    } );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }

                // All records matched, and there are no more expected records.
                return matched.get() && !expected.hasNext();
            }

            @Override
            public void describeTo( Description description )
            {
                description
                        .appendText( "Stream[" )
                        .appendValueList( " fieldNames=[", ",", "]", fieldNames )
                        .appendList( ", records=[", ",", "]", asList( records ) );
            }
        };
    }
}
