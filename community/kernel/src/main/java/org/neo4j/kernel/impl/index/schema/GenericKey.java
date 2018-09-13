/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

/**
 * Key abstraction to handle single or composite keys.
 */
abstract class GenericKey extends NativeIndexKey<GenericKey>
{
    GenericKey( IndexSpecificSpaceFillingCurveSettingsCache spatialSettings )
    {
        super( spatialSettings );
    }

    @Override
    void assertValidValue( int stateSlot, Value value )
    {
        // No need, we can handle all values
    }

    abstract void initFromDerivedSpatialValue( int stateSlot, CoordinateReferenceSystem crs, long derivedValue, Inclusion inclusion );

    abstract void initAsPrefixLow( int stateSlot, String prefix );

    abstract void initAsPrefixHigh( int stateSlot, String prefix );

    abstract void copyValuesFrom( GenericKey key );

    abstract int size();

    abstract void write( PageCursor cursor );

    abstract boolean read( PageCursor cursor, int keySize );

    abstract void minimalSplitter( GenericKey left, GenericKey right, GenericKey into );

    abstract GenericKeyState stateSlot( int slot );
}
