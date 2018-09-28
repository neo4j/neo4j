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

import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ValueGroup;

import static java.util.Arrays.copyOf;
import static org.neo4j.values.storable.UTF8StringValue.byteArrayCompare;

class StringValueCreatorUtil extends ValueCreatorUtil<StringIndexKey,NativeIndexValue>
{
    StringValueCreatorUtil( StoreIndexDescriptor schemaIndexDescriptor )
    {
        super( schemaIndexDescriptor, RandomValues.typesOfGroup( ValueGroup.TEXT ) );
    }

    @Override
    int compareIndexedPropertyValue( StringIndexKey key1, StringIndexKey key2 )
    {
        return byteArrayCompare(
                copyOf( key1.bytes, key1.bytesLength ),
                copyOf( key2.bytes, key2.bytesLength ) );
    }
}
