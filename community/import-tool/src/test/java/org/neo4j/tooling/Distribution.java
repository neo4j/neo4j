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
package org.neo4j.tooling;

import java.util.Random;

/**
 * Distributes the given items so that item[0] converges towards being returned 1/2 of the times,
 * the next item, item[1] 1/4 of the times, item[2] 1/8 and so on.
 */
public class Distribution<T>
{
    private final T[] items;

    public Distribution( T[] items )
    {
        this.items = items;
    }

    public T random( Random random )
    {
        float value = random.nextFloat();
        float comparison = 0.5f;
        for ( int i = 0; i < items.length; i++ )
        {
            if ( value >= comparison )
            {
                return items[i];
            }
            comparison /= 2f;
        }
        return items[items.length-1];
    }
}
