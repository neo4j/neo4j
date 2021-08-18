/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;

class GenericLayout extends IndexLayout<BtreeKey>
{
    private final int numberOfSlots;
    private final IndexSpecificSpaceFillingCurveSettings spatialSettings;

    GenericLayout( int numberOfSlots, IndexSpecificSpaceFillingCurveSettings spatialSettings )
    {
        super( false, Layout.namedIdentifier( "NSIL", numberOfSlots ), 0, 5 );
        this.numberOfSlots = numberOfSlots;
        this.spatialSettings = spatialSettings;
    }

    @Override
    public BtreeKey newKey()
    {
        return numberOfSlots == 1
               // An optimized version which has the GenericKeyState built-in w/o indirection
               ? new BtreeKey( spatialSettings )
               // A version which has an indirection to GenericKeyState[]
               : new CompositeBtreeKey( numberOfSlots, spatialSettings );
    }

    @Override
    public BtreeKey copyKey( BtreeKey key, BtreeKey into )
    {
        into.copyFrom( key );
        return into;
    }

    @Override
    public int keySize( BtreeKey key )
    {
        return key.size();
    }

    @Override
    public void writeKey( PageCursor cursor, BtreeKey key )
    {
        key.put( cursor );
    }

    @Override
    public void readKey( PageCursor cursor, BtreeKey into, int keySize )
    {
        into.get( cursor, keySize );
    }

    @Override
    public void minimalSplitter( BtreeKey left, BtreeKey right, BtreeKey into )
    {
        right.minimalSplitter( left, right, into );
    }

    IndexSpecificSpaceFillingCurveSettings getSpaceFillingCurveSettings()
    {
        return spatialSettings;
    }

    @Override
    public void initializeAsLowest( BtreeKey key )
    {
        key.initValuesAsLowest();
    }

    @Override
    public void initializeAsHighest( BtreeKey key )
    {
        key.initValuesAsHighest();
    }
}
