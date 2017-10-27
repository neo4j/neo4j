/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.index.schema.LayoutTestUtil.countUniqueValues;
import static org.neo4j.values.storable.Values.values;

public class FullScanNonUniqueIndexSamplerTest extends SchemaNumberIndexTestUtil<SchemaNumberKey,SchemaNumberValue>
{
    @Test
    public void shouldIncludeAllValuesInTree() throws Exception
    {
        // GIVEN
        Number[] values = generateNumberValues();
        buildTree( values );

        // WHEN
        IndexSample sample;
        try ( GBPTree<SchemaNumberKey,SchemaNumberValue> gbpTree = getTree() )
        {
            IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
            FullScanNonUniqueIndexSampler<SchemaNumberKey,SchemaNumberValue> sampler =
                    new FullScanNonUniqueIndexSampler<>( gbpTree, layout, samplingConfig );
            sample = sampler.result();
        }

        // THEN
        assertEquals( values.length, sample.sampleSize() );
        assertEquals( countUniqueValues( values ), sample.uniqueValues() );
        assertEquals( values.length, sample.indexSize() );
    }

    private Number[] generateNumberValues()
    {
        IndexEntryUpdate<IndexDescriptor>[] updates = layoutUtil.someUpdates();
        Number[] result = new Number[updates.length];
        for ( int i = 0; i < updates.length; i++ )
        {
            result[i] = (Number) updates[i].values()[0].asObject();
        }
        return result;
    }

    private void buildTree( Number[] values ) throws IOException
    {
        try ( GBPTree<SchemaNumberKey,SchemaNumberValue> gbpTree = getTree() )
        {
            try ( Writer<SchemaNumberKey,SchemaNumberValue> writer = gbpTree.writer() )
            {
                SchemaNumberKey key = layout.newKey();
                SchemaNumberValue value = layout.newValue();
                long nodeId = 0;
                for ( Number number : values )
                {
                    key.from( nodeId, values( number ) );
                    value.from( values( number ) );
                    writer.put( key, value );
                    nodeId++;
                }
            }
            gbpTree.checkpoint( IOLimiter.unlimited() );
        }
    }

    @Override
    protected LayoutTestUtil<SchemaNumberKey,SchemaNumberValue> createLayoutTestUtil()
    {
        return new NonUniqueLayoutTestUtil();
    }
}
