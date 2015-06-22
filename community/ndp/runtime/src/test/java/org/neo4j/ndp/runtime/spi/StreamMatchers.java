/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.runtime.spi;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;

public class StreamMatchers
{
    public static Matcher<Record> eqRecord( final Matcher<?>... expectedFieldValues )
    {
        return new TypeSafeMatcher<Record>()
        {
            @Override
            protected boolean matchesSafely( Record item )
            {
                if(expectedFieldValues.length != item.fields().length)
                {
                    return false;
                }

                for ( int i = 0; i < item.fields().length; i++ )
                {
                    if(!expectedFieldValues[i].matches( item.fields()[i] ))
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
                           .appendList( ", fields=[", ",", "]", asList(expectedFieldValues) );

            }
        };
    }

    public static Matcher<RecordStream> equalsStream( final String[] fieldNames, final Matcher ... records )
    {
        return new TypeSafeMatcher<RecordStream>()
        {
            @Override
            protected boolean matchesSafely( RecordStream item )
            {
                if(!Arrays.equals(fieldNames, item.fieldNames()))
                {
                    return false;
                }
                final Iterator<Matcher> expected = asList( records ).iterator();
                final AtomicBoolean matched = new AtomicBoolean( true );
                try
                {
                    item.accept( new RecordStream.Visitor()
                    {
                        @Override
                        public void visit( Record record )
                        {
                            if ( !expected.hasNext() || !expected.next().matches( record ) )
                            {
                                matched.set( false );
                            }
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
                        .appendValueList( " fieldNames=[",",","]", fieldNames )
                        .appendList( ", records=[", ",", "]", asList( records ) );
            }
        };
    }
}
