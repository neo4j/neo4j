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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class MapMatcher implements ValueMatcher
{
    public static final MapMatcher EMPTY = new MapMatcher( Collections.emptyMap() );

    private final Map<String,ValueMatcher> map;

    public MapMatcher( Map<String,ValueMatcher> map )
    {
        this.map = map;
    }

    @Override
    public boolean matches( Object value )
    {
        if ( value instanceof Map )
        {
            Map realMap = (Map) value;
            Set<String> expectedKeys = map.keySet();
            Set actualKeys = realMap.keySet();

            if ( expectedKeys.equals( actualKeys ) )
            {
                for ( Map.Entry<String,ValueMatcher> entry : map.entrySet() )
                {
                    if ( !entry.getValue().matches( realMap.get( entry.getKey() ) ) )
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "MapMatcher" + map.toString();
    }
}
