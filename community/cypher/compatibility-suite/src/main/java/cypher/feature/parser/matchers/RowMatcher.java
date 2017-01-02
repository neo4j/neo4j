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

import java.util.Map;
import java.util.Set;

public class RowMatcher implements Matcher<Map<String,Object>>
{
    private final Map<String,ValueMatcher> values;

    public RowMatcher( Map<String,ValueMatcher> values )
    {
        this.values = values;
    }

    @Override
    public boolean matches( Map<String,Object> value )
    {
        Set<String> keys = values.keySet();
        if ( keys.equals( value.keySet() ) )
        {
            for ( String key : keys )
            {
                if ( !values.get( key ).matches( value.get( key ) ) )
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
        return "RowMatcher" + values;
    }
}
