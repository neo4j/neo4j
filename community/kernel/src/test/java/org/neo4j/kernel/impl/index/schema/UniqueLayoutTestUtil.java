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

import java.util.List;
import java.util.Set;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

class UniqueLayoutTestUtil<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> extends LayoutTestUtil<KEY, VALUE>
{

    private final LayoutTestUtil<KEY, VALUE> delegate;

    UniqueLayoutTestUtil( LayoutTestUtil<KEY, VALUE> delegate )
    {
        super( delegate.schemaIndexDescriptor );
        this.delegate = delegate;
    }

    @Override
    Layout<KEY,VALUE> createLayout()
    {
        return delegate.createLayout();
    }

    @Override
    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdates()
    {
        return delegate.someUpdatesNoDuplicateValues();
    }

    @Override
    IndexQuery rangeQuery( Value from, boolean fromInclusive, Value to, boolean toInclusive )
    {
        return delegate.rangeQuery( from, fromInclusive, to, toInclusive );
    }

    @Override
    int compareIndexedPropertyValue( KEY key1, KEY key2 )
    {
        return delegate.compareIndexedPropertyValue( key1, key2 );
    }

    @Override
    Value newUniqueValue( RandomRule random, Set<Object> uniqueCompareValues, List<Value> uniqueValues )
    {
        return delegate.newUniqueValue( random, uniqueCompareValues, uniqueValues );
    }

    @Override
    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdatesNoDuplicateValues()
    {
        return delegate.someUpdatesNoDuplicateValues();
    }

    @Override
    IndexEntryUpdate<SchemaIndexDescriptor>[] someUpdatesWithDuplicateValues()
    {
        return delegate.someUpdatesWithDuplicateValues();
    }

    @Override
    protected double fractionDuplicates()
    {
        return 0.0;
    }
}
