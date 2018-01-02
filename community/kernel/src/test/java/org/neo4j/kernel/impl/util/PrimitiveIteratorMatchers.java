/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

import static junit.framework.TestCase.fail;

import static org.neo4j.helpers.collection.IteratorUtil.asList;

public class PrimitiveIteratorMatchers
{
    // Unordered!
    public static Matcher<PrimitiveLongIterator> containsLongs(final long ... longs)
    {
        return new TypeSafeMatcher<PrimitiveLongIterator>()
        {
            @Override
            protected boolean matchesSafely( PrimitiveLongIterator item )
            {
                List<Long> actual = asList( item );
                List<Long> expected = expected();

                eachActual:
                for ( Long actualValue : actual )
                {
                    Iterator<Long> expectedIt = expected.iterator();
                    while(expectedIt.hasNext())
                    {
                        if(expectedIt.next().equals( actualValue ))
                        {
                            expectedIt.remove();
                            continue eachActual;
                        }
                    }

                    fail( "Got unexpected value: " + actualValue );
                }

                return expected.size() == 0;
            }

            private List<Long> expected()
            {
                List<Long> expected = new ArrayList<>();
                for ( long v : longs )
                {
                    expected.add( v );
                }
                return expected;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValueList( "[", ",", "]", expected() );
            }
        };
    }

}
