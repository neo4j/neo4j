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

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;

/**
 * {@link NativeSchemaIndexPopulator} which enforces unique values.
 */
class NativeUniqueSchemaIndexPopulator<KEY extends NativeSchemaKey, VALUE extends NativeSchemaValue>
        extends NativeSchemaIndexPopulator<KEY,VALUE>
{
    private final UniqueIndexSampler sampler;
    private final IndexSamplingConfig samplingConfig;

    NativeUniqueSchemaIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File storeFile, Layout<KEY,VALUE> layout,
            IndexSamplingConfig samplingConfig, SchemaIndexProvider.Monitor monitor, IndexDescriptor descriptor, long indexId )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor, indexId );
        this.samplingConfig = samplingConfig;
        this.sampler = new UniqueIndexSampler();
    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        sampler.increment( 1 );
    }

    @Override
    public IndexSample sampleResult()
    {
        return sampler.result();
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor accessor )
    {
        // The index population detects conflicts on the fly, however for updates coming in we're in a position
        // where we cannot detect conflicts while applying, but instead afterwards.
        return new DeferredConflictCheckingIndexUpdater( super.newPopulatingUpdater( accessor ),
                () -> new NumberSchemaIndexReader( tree, layout, samplingConfig, descriptor ) {}, descriptor );
    }
}
