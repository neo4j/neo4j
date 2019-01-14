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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;

class GenericLayout extends IndexLayout<GenericKey,NativeIndexValue>
{
    private final int numberOfSlots;
    private final IndexSpecificSpaceFillingCurveSettingsCache spatialSettings;

    GenericLayout( int numberOfSlots, IndexSpecificSpaceFillingCurveSettingsCache spatialSettings )
    {
        super( "NSIL", 0, 5 );
        this.numberOfSlots = numberOfSlots;
        this.spatialSettings = spatialSettings;
    }

    @Override
    public GenericKey newKey()
    {
        return numberOfSlots == 1
               // An optimized version which has the GenericKeyState built-in w/o indirection
               ? new GenericKey( spatialSettings )
               // A version which has an indirection to GenericKeyState[]
               : new CompositeGenericKey( numberOfSlots, spatialSettings );
    }

    @Override
    public GenericKey copyKey( GenericKey key, GenericKey into )
    {
        into.copyFrom( key );
        return into;
    }

    @Override
    public int keySize( GenericKey key )
    {
        return key.size();
    }

    @Override
    public void writeKey( PageCursor cursor, GenericKey key )
    {
        key.put( cursor );
    }

    @Override
    public void readKey( PageCursor cursor, GenericKey into, int keySize )
    {
        into.get( cursor, keySize );
    }

    @Override
    public boolean fixedSize()
    {
        return false;
    }

    @Override
    public void minimalSplitter( GenericKey left, GenericKey right, GenericKey into )
    {
        right.minimalSplitter( left, right, into );
    }

    IndexSpecificSpaceFillingCurveSettingsCache getSpaceFillingCurveSettings()
    {
        return spatialSettings;
    }
}
