/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.parser.matchers;

import java.lang.reflect.Array;
import java.util.List;
import java.util.function.Function;

public class ListMatcher implements ValueMatcher
{
    protected final List<ValueMatcher> list;

    public ListMatcher( List<ValueMatcher> list )
    {
        this.list = list;
    }

    @Override
    public boolean matches( Object value )
    {
        if ( value == null )
        {
            return false;
        }
        else if ( value instanceof List )
        {
            List realList = (List) value;
            return sizeAndElements( realList.size(), realList::get );
        }
        else if ( value.getClass().isArray() )
        {
            return sizeAndElements( Array.getLength( value ), integer -> Array.get( value, integer ) );
        }
        return false;
    }

    protected boolean sizeAndElements( int length, Function<Integer,Object> resultList )
    {
        if ( list.size() == length )
        {
            for ( int i = 0; i < length; ++i )
            {
                if ( !list.get( i ).matches( resultList.apply( i ) ) )
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return list.toString();
    }
}
