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

import java.io.File;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;

public class StringSchemaIndexPopulator extends NativeSchemaIndexPopulator<StringSchemaKey,NativeSchemaValue>
{
    StringSchemaIndexPopulator( PageCache pageCache, FileSystemAbstraction fs, File storeFile, Layout<StringSchemaKey,NativeSchemaValue> layout,
                                IndexProvider.Monitor monitor, SchemaIndexDescriptor descriptor, long indexId, IndexSamplingConfig samplingConfig )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor, indexId, samplingConfig );
    }

    @Override
    IndexReader newReader()
    {
        return new StringSchemaIndexReader( tree, layout, samplingConfig, descriptor );
    }
}
