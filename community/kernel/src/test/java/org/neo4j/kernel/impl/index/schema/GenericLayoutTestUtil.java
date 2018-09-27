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

import org.apache.commons.lang3.ArrayUtils;

import java.util.HashMap;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Values;

class GenericLayoutTestUtil extends LayoutTestUtil<CompositeGenericKey,NativeIndexValue>
{
    // todo This is only here to have SOME values to start with. It will be replaced by random value generation later on.
    private static final Number[] SOME_VALUE = new Number[]
            {
                    Byte.MAX_VALUE,
                    Byte.MIN_VALUE,
                    Short.MAX_VALUE,
                    Short.MIN_VALUE,
                    Integer.MAX_VALUE,
                    Integer.MIN_VALUE,
                    Long.MAX_VALUE,
                    Long.MIN_VALUE,
                    Float.MAX_VALUE,
                    -Float.MAX_VALUE,
                    Double.MAX_VALUE,
                    -Double.MAX_VALUE,
                    Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY,
                    0,
                    // These two values below coerce to the same double
                    1234567890123456788L,
                    1234567890123456789L
            };

    private IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings =
            new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), new HashMap<>() );
    StandardConfiguration configuration = new StandardConfiguration();

    GenericLayoutTestUtil( IndexDescriptor indexDescriptor )
    {
        super( indexDescriptor );
    }

    @Override
    IndexLayout<CompositeGenericKey,NativeIndexValue> createLayout()
    {
        return new GenericLayout( 1, spaceFillingCurveSettings );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdates()
    {
        return someUpdatesWithDuplicateValues();
    }

    @Override
    RandomValues.Type[] supportedTypes()
    {
        return RandomValues.Type.values();
    }

    @Override
    int compareIndexedPropertyValue( CompositeGenericKey key1, CompositeGenericKey key2 )
    {
        return Values.COMPARATOR.compare( key1.asValues()[0], key2.asValues()[0] );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdatesNoDuplicateValues()
    {
        return generateAddUpdatesFor( SOME_VALUE );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdatesWithDuplicateValues()
    {
        return generateAddUpdatesFor( ArrayUtils.addAll( SOME_VALUE, SOME_VALUE ) );
    }
}
