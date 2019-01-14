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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

abstract class IndexLayout<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends Layout.Adapter<KEY,VALUE>
{
    private final long identifier;
    private final int majorVersion;
    private final int minorVersion;

    // allows more control of the identifier, needed for legacy reasons for the two number layouts
    IndexLayout( long identifier, int majorVersion, int minorVersion )
    {
        this.identifier = identifier;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    IndexLayout( String layoutName, int majorVersion, int minorVersion )
    {
        this( Layout.namedIdentifier( layoutName, NativeIndexValue.SIZE ), majorVersion, minorVersion );
    }

    @Override
    public VALUE newValue()
    {
        return (VALUE) NativeIndexValue.INSTANCE;
    }

    @Override
    public int valueSize( NativeIndexValue nativeIndexValue )
    {
        return NativeIndexValue.SIZE;
    }

    @Override
    public void writeValue( PageCursor cursor, NativeIndexValue nativeIndexValue )
    {
        // nothing to write
    }

    @Override
    public void readValue( PageCursor cursor, NativeIndexValue into, int valueSize )
    {
        // nothing to read
    }

    @Override
    public boolean fixedSize()
    {
        return true; // for the most case
    }

    @Override
    public long identifier()
    {
        return identifier;
    }

    @Override
    public int majorVersion()
    {
        return majorVersion;
    }

    @Override
    public int minorVersion()
    {
        return minorVersion;
    }

    @Override
    public final int compare( KEY o1, KEY o2 )
    {
        int valueComparison = compareValue( o1, o2 );
        if ( valueComparison == 0 )
        {
            // This is a special case where we need also compare entityId to support inclusive/exclusive
            if ( o1.getCompareId() & o2.getCompareId() )
            {
                return Long.compare( o1.getEntityId(), o2.getEntityId() );
            }
        }
        return valueComparison;
    }

    int compareValue( KEY o1, KEY o2 )
    {
        return o1.compareValueTo( o2 );
    }
}
