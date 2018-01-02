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
package org.neo4j.kernel.impl.api;

import java.util.Comparator;

public abstract class PropertyValueComparator<T> implements Comparator<T>
{
    public boolean isEmptyRange( T lower, boolean includeLower, T upper, boolean includeUpper )
    {
        if ( lower == null || upper == null )
        {
            return false;
        }
        else
        {
            int cmp = compare( lower, upper );

            if ( includeLower && includeUpper )
            {
                // l <= .. <= u <=> l <= u <=> empty if l > u
                return cmp > 0;
            }
            else
            {
                // l <= .. < u <=> l < u <=> empty if l >= u
                // l < .. <= u <=> l < u <=> empty if l >= u
                // l < .. < u <=> l < u <=> empty if l >= u
                return cmp >= 0;
            }
        }
    }
}
