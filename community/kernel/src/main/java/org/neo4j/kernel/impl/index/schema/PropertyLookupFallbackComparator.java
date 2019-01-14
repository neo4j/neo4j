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

import java.util.Comparator;

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.values.storable.Values;

/**
 * Compares {@link NativeSchemaKey}, but will consult {@link PropertyAccessor} on coming across a comparison of zero.
 * This is useful for e.g. spatial keys which are indexed lossily.
 * @param <KEY> type of index key.
 */
class PropertyLookupFallbackComparator<KEY extends NativeSchemaKey<KEY>> implements Comparator<KEY>
{
    private final SchemaLayout<KEY> schemaLayout;
    private final PropertyAccessor propertyAccessor;
    private final int propertyKeyId;

    PropertyLookupFallbackComparator( SchemaLayout<KEY> schemaLayout, PropertyAccessor propertyAccessor, int propertyKeyId )
    {
        this.schemaLayout = schemaLayout;
        this.propertyAccessor = propertyAccessor;
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public int compare( KEY k1, KEY k2 )
    {
        int comparison = schemaLayout.compareValue( k1, k2 );
        if ( comparison != 0 )
        {
            return comparison;
        }
        try
        {
            return Values.COMPARATOR.compare(
                    propertyAccessor.getPropertyValue( k1.getEntityId(), propertyKeyId ),
                    propertyAccessor.getPropertyValue( k2.getEntityId(), propertyKeyId ) );
        }
        catch ( EntityNotFoundException e )
        {
            // We don't want this operation to fail since it's merely counting distinct values.
            // This entity not being there is most likely a result of a concurrent deletion happening as we speak.
            return comparison;
        }
    }
}
