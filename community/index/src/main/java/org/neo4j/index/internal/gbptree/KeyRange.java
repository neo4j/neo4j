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
package org.neo4j.index.internal.gbptree;

import java.util.Comparator;

import static java.lang.String.format;

class KeyRange<KEY>
{
    private final Comparator<KEY> comparator;
    private final KEY fromInclusive;
    private final KEY toExclusive;
    private final Layout<KEY,?> layout;
    private final KeyRange<KEY> superRange;

    KeyRange( Comparator<KEY> comparator, KEY fromInclusive, KEY toExclusive, Layout<KEY,?> layout,
            KeyRange<KEY> superRange )
    {
        this.comparator = comparator;
        this.superRange = superRange;
        this.fromInclusive = fromInclusive == null ? null : layout.copyKey( fromInclusive, layout.newKey() );
        this.toExclusive = toExclusive == null ? null : layout.copyKey( toExclusive, layout.newKey() );
        this.layout = layout;
    }

    boolean inRange( KEY key )
    {
        if ( fromInclusive != null )
        {
            if ( toExclusive != null )
            {
                return comparator.compare( key, fromInclusive ) >= 0 && comparator.compare( key, toExclusive ) < 0;
            }
            return comparator.compare( key, fromInclusive ) >= 0;
        }
        return toExclusive == null || comparator.compare( key, toExclusive ) < 0;
    }

    KeyRange<KEY> narrowLeft( KEY left )
    {
        if ( fromInclusive == null )
        {
            return new KeyRange<>( comparator, left, toExclusive, layout, superRange );
        }
        if ( left == null )
        {
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, superRange );
        }
        if ( comparator.compare( fromInclusive, left ) < 0 )
        {
            return new KeyRange<>( comparator, left, toExclusive, layout, superRange );
        }
        return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, superRange );
    }

    KeyRange<KEY> restrictLeft( KEY left )
    {
        if ( fromInclusive == null )
        {
            return new KeyRange<>( comparator, left, toExclusive, layout, this );
        }
        if ( left == null )
        {
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
        }
        if ( comparator.compare( fromInclusive, left ) < 0 )
        {
            return new KeyRange<>( comparator, left, toExclusive, layout, this );
        }
        return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
    }

    KeyRange<KEY> restrictRight( KEY right )
    {
        if ( toExclusive == null )
        {
            return new KeyRange<>( comparator, fromInclusive, right, layout, this );
        }
        if ( right == null )
        {
            return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
        }
        if ( comparator.compare( toExclusive, right ) > 0 )
        {
            return new KeyRange<>( comparator, fromInclusive, right, layout, this );
        }
        return new KeyRange<>( comparator, fromInclusive, toExclusive, layout, this );
    }

    @Override
    public String toString()
    {
        return (superRange != null ? format( "%s%n", superRange ) : "") + fromInclusive + " ≤ key < " + toExclusive;
    }
}
