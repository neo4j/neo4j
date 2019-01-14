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
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

/**
 * {@link IndexAccessor} using {@link StringLayout}, i.e for {@link String} values.
 */
public class StringSchemaIndexAccessor extends NativeSchemaIndexAccessor<StringSchemaKey,NativeSchemaValue>
{
    private Validator<Value> validator;

    StringSchemaIndexAccessor(
            PageCache pageCache,
            FileSystemAbstraction fs,
            File storeFile,
            Layout<StringSchemaKey,NativeSchemaValue> layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            IndexProvider.Monitor monitor,
            SchemaIndexDescriptor descriptor,
            long indexId,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        super( pageCache, fs, storeFile, layout, recoveryCleanupWorkCollector, monitor, descriptor, indexId, samplingConfig );
    }

    @Override
    protected void afterTreeInstantiation( GBPTree<StringSchemaKey,NativeSchemaValue> tree )
    {
        validator = new NativeIndexKeyLengthValidator<>( tree.keyValueSizeCap(), layout );
    }

    @Override
    public IndexReader newReader()
    {
        assertOpen();
        return new StringSchemaIndexReader( tree, layout, samplingConfig, descriptor );
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        validator.validate( tuple[0] );
    }
}
