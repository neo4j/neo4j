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
package org.neo4j.kernel.api.impl.schema;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Arrays;

// TODO: move to common test module
class LongArrayMatcher extends TypeSafeDiagnosingMatcher<long[]>
{

    public static LongArrayMatcher emptyArrayMatcher()
    {
        return new LongArrayMatcher( new long[]{} );
    }

    public static LongArrayMatcher of( long... values )
    {
        return new LongArrayMatcher( values );
    }

    private long[] expectedArray;

    LongArrayMatcher( long[] expectedArray )
    {
        this.expectedArray = expectedArray;
    }

    @Override
    protected boolean matchesSafely( long[] items, Description mismatchDescription )
    {
        describeArray( items, mismatchDescription );
        if ( items.length != expectedArray.length )
        {
            return false;
        }
        for ( int i = 0; i < items.length; i++ )
        {
            if ( items[i] != expectedArray[i] )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void describeTo( Description description )
    {
        describeArray( expectedArray, description );
    }

    private void describeArray( long[] value, Description description )
    {
        description.appendText( "long[]" ).appendText( Arrays.toString( value ) );
    }
}
