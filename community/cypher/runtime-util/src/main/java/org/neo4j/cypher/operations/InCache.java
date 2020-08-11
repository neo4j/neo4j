/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.operations;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;

import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;

public class InCache
{
        private final LinkedHashMap<ListValue,InCacheChecker> seen;

        public InCache()
        {
            this( 16 );
        }

        public InCache( int maxSize )
        {
            seen = new LinkedHashMap<>( maxSize >> 2, 0.75f, true )
            {
                @Override
                protected boolean removeEldestEntry( Map.Entry<ListValue,InCacheChecker> eldest )
                {
                    return super.size() > maxSize;
                }
            };
        }

    public Value check( AnyValue value, ListValue list )
    {
        if ( list.isEmpty() )
        {
            return FALSE;
        }
        else if ( value == NO_VALUE )
        {
            return NO_VALUE;
        }
        else
        {
            InCacheChecker checker = seen.computeIfAbsent( list,
                                                           k -> new InCacheChecker( list.iterator() ) );
            return checker.check( value );
        }
    }

    private static class InCacheChecker
    {
        private final Set<AnyValue> seen = new HashSet<>();
        private final Iterator<AnyValue> iterator;
        private boolean seenUndefined;

        private InCacheChecker( Iterator<AnyValue> iterator )
        {
            this.iterator = iterator;
        }

        private Value check( AnyValue value )
        {
            if ( seen.contains( value ) )
            {
                return TRUE;
            }
            else
            {
                while ( iterator.hasNext() )
                {
                    var next = iterator.next();
                    if ( next == NO_VALUE )
                    {
                        seenUndefined = true;
                    }
                    else
                    {
                        seen.add( next );
                        switch ( next.ternaryEquals( value ) )
                        {
                        case TRUE:
                            return TRUE;
                        case UNDEFINED:
                            seenUndefined = true;
                        case FALSE:
                            break;
                        default:
                            throw new IllegalStateException( "Unknown state" );
                        }
                    }
                }
                return seenUndefined ? NO_VALUE : FALSE;
            }
        }
    }
}
