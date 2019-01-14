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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.countUniqueValues;
import static org.neo4j.values.storable.RandomValues.typesOfGroup;
import static org.neo4j.values.storable.ValueGroup.NUMBER;

public class NumberFullScanNonUniqueIndexSamplerTest extends NativeIndexTestUtil<NumberIndexKey,NativeIndexValue>
{
    @Test
    public void shouldIncludeAllValuesInTree() throws Exception
    {
        // GIVEN
        Value[] values = generateNumberValues();
        buildTree( values );

        // WHEN
        IndexSample sample;
        try ( GBPTree<NumberIndexKey,NativeIndexValue> gbpTree = getTree() )
        {
            FullScanNonUniqueIndexSampler<NumberIndexKey,NativeIndexValue> sampler =
                    new FullScanNonUniqueIndexSampler<>( gbpTree, layout );
            sample = sampler.result();
        }

        // THEN
        assertEquals( values.length, sample.sampleSize() );
        assertEquals( countUniqueValues( values ), sample.uniqueValues() );
        assertEquals( values.length, sample.indexSize() );
    }

    private Value[] generateNumberValues()
    {
        ValueType[] numberTypes = RandomValues.including( t -> t.valueGroup == NUMBER );
        int size = 20;
        Value[] result = new NumberValue[size];
        for ( int i = 0; i < size; i++ )
        {
            result[i] = random.randomValues().nextValueOfTypes( numberTypes );
        }
        return result;
    }

    private void buildTree( Value[] values ) throws IOException
    {
        try ( GBPTree<NumberIndexKey,NativeIndexValue> gbpTree = getTree() )
        {
            try ( Writer<NumberIndexKey,NativeIndexValue> writer = gbpTree.writer() )
            {
                NumberIndexKey key = layout.newKey();
                NativeIndexValue value = layout.newValue();
                long nodeId = 0;
                for ( Value number : values )
                {
                    key.initialize( nodeId );
                    key.initFromValue( 0, number, NEUTRAL );
                    value.from( number );
                    writer.put( key, value );
                    nodeId++;
                }
            }
            gbpTree.checkpoint( IOLimiter.UNLIMITED );
        }
    }

    @Override
    protected ValueCreatorUtil<NumberIndexKey,NativeIndexValue> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( forLabel( 42, 666 ).withId( 0 ), typesOfGroup( NUMBER ), FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexLayout<NumberIndexKey,NativeIndexValue> createLayout()
    {
        return new NumberLayoutNonUnique();
    }
}
