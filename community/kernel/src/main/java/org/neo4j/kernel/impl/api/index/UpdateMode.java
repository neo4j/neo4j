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
package org.neo4j.kernel.impl.api.index;

import static java.util.Arrays.binarySearch;

public enum UpdateMode
{
    ADDED
    {
        @Override
        public boolean forLabel( long[] before, long[] after, long label )
        {
            return binarySearch( after, label ) >= 0;
        }
    },
    CHANGED
    {
        @Override
        public boolean forLabel( long[] before, long[] after, long label )
        {
            return ADDED.forLabel( before, after, label ) && REMOVED.forLabel( before, after, label );
        }
    },
    REMOVED
    {
        @Override
        public boolean forLabel( long[] before, long[] after, long label )
        {
            return binarySearch( before, label ) >= 0;
        }
    };
    
    public abstract boolean forLabel( long[] before, long[] after, long label );
}
