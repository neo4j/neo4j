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
package cypher.feature.parser.matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class UnorderedListMatcher extends ListMatcher
{
    public UnorderedListMatcher( List<ValueMatcher> list )
    {
        super( list );
    }

    @Override
    protected boolean sizeAndElements( int length, Function<Integer,Object> resultList )
    {
        if ( list.size() == length )
        {
            List<ValueMatcher> mutableCopy = new ArrayList<>( list );
            for ( int i = 0; i < length; ++i )
            {
                Object value = resultList.apply( i );
                int index = findMatch( mutableCopy, value );
                if ( index < 0 )
                {
                    return false;
                }
                mutableCopy.remove( index );
            }
            return true;
        }
        return false;
    }

    /**
     * Searches the input list for a matcher that matches the input value.
     *
     * @param list the list of matchers to match the value against.
     * @param value the value to match.
     * @param <T> the type of the value.
     * @return the index of the first found matcher that matched the value, or -1 if none was found.
     */
    static <T> int findMatch( List<? extends Matcher<T>> list, T value )
    {
        for ( int j = 0; j < list.size(); ++j )
        {
            if ( list.get( j ).matches( value ) )
            {
                return j;
            }
        }
        return -1;
    }
}
