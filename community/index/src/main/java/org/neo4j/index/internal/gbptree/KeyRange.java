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
package org.neo4j.index.internal.gbptree;

import java.util.Comparator;

import static java.lang.String.format;

class KeyRange<KEY>
{
    private final int level;
    private final long pageId;
    private final Comparator<KEY> comparator;
    private final KEY fromInclusive;
    private final KEY toExclusive;
    private final Layout<KEY,?> layout;
    private final KeyRange<KEY> superRange;

    KeyRange( int level, long pageId, Comparator<KEY> comparator, KEY fromInclusive, KEY toExclusive, Layout<KEY,?> layout,
            KeyRange<KEY> superRange )
    {
        this.level = level;
        this.pageId = pageId;
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

    KeyRange<KEY> newSubRange( int level, long pageId )
    {
        return new KeyRange<>( level, pageId, comparator, fromInclusive, toExclusive, layout, this );
    }

    boolean hasPageIdInStack( long pageId )
    {
        if ( this.pageId == pageId )
        {
            return true;
        }
        if ( superRange != null )
        {
            return superRange.hasPageIdInStack( pageId );
        }
        return false;
    }

    KeyRange<KEY> restrictLeft( KEY left )
    {
        KEY newLeft;
        if ( fromInclusive == null )
        {
            newLeft = left;
        }
        else if ( left == null )
        {
            newLeft = fromInclusive;
        }
        else if ( comparator.compare( fromInclusive, left ) < 0 )
        {
            newLeft = left;
        }
        else
        {
            newLeft = fromInclusive;
        }
        return new KeyRange<>( level, pageId, comparator, newLeft, toExclusive, layout, superRange );
    }

    KeyRange<KEY> restrictRight( KEY right )
    {
        KEY newRight;
        if ( toExclusive == null )
        {
            newRight = right;
        }
        else if ( right == null )
        {
            newRight = toExclusive;
        }
        else if ( comparator.compare( toExclusive, right ) > 0 )
        {
            newRight = right;
        }
        else
        {
            newRight = toExclusive;
        }
        return new KeyRange<>( level, pageId, comparator, fromInclusive, newRight, layout, superRange );
    }

    @Override
    public String toString()
    {
        return (superRange != null ? format( "%s%n", superRange ) : "") + singleLevel();
    }

    private String singleLevel()
    {
        return "level: " + level + " {" + pageId + "} " + fromInclusive + " ≤ key < " + toExclusive;
    }
}
