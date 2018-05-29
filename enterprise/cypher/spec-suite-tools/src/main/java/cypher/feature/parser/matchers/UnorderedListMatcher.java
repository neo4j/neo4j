/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
