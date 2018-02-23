/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;

import static org.neo4j.values.storable.StringsLibrary.STRINGS;
import static org.neo4j.values.storable.UTF8StringValue.codePointByteArrayCompare;

abstract class StringLayoutTestUtil extends LayoutTestUtil<StringSchemaKey,NativeSchemaValue>
{
    StringLayoutTestUtil( IndexDescriptor indexDescriptor )
    {
        super( indexDescriptor );
    }

    @Override
    IndexQuery rangeQuery( Object from, boolean fromInclusive, Object to, boolean toInclusive )
    {
        return IndexQuery.range( 0, (String) from, fromInclusive, (String) to, toInclusive );
    }

    @Override
    int compareIndexedPropertyValue( StringSchemaKey key1, StringSchemaKey key2 )
    {
        return codePointByteArrayCompare( key1.bytes, key2.bytes );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdates()
    {
        return generateAddUpdatesFor( STRINGS );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdatesNoDuplicateValues()
    {
        return generateAddUpdatesFor( STRINGS );
    }

    @Override
    IndexEntryUpdate<IndexDescriptor>[] someUpdatesWithDuplicateValues()
    {
        Collection<String> duplicates = new ArrayList<>( asList( STRINGS ) );
        duplicates.addAll( asList( STRINGS ) );
        return generateAddUpdatesFor( duplicates.toArray() );
    }

    @Override
    protected Value newUniqueValue( RandomRule random, Set<Object> uniqueCompareValues, List<Value> uniqueValues )
    {
        String candidate;
        do
        {
            candidate = random.string();
        }
        while ( !uniqueCompareValues.add( candidate ) );
        TextValue result = Values.stringValue( candidate );
        uniqueValues.add( result );
        return result;
    }
}
