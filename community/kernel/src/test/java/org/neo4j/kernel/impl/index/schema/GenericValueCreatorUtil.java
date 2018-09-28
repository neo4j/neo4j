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

import java.util.HashMap;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Values;

class GenericValueCreatorUtil extends ValueCreatorUtil<CompositeGenericKey,NativeIndexValue>
{
    private IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings =
            new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), new HashMap<>() );
    StandardConfiguration configuration = new StandardConfiguration();

    GenericValueCreatorUtil( StoreIndexDescriptor indexDescriptor )
    {
        super( indexDescriptor );
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
}
