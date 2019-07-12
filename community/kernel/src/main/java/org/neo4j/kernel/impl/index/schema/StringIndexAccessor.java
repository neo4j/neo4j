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

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.Value;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;

/**
 * {@link IndexAccessor} using {@link StringLayout}, i.e for {@link String} values.
 */
public class StringIndexAccessor extends NativeIndexAccessor<StringIndexKey,NativeIndexValue>
{
    private Validator<Value> validator;

    StringIndexAccessor( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<StringIndexKey,NativeIndexValue> layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor, boolean readOnly )
    {
        super( pageCache, fs, storeFile, layout, monitor, descriptor, NO_HEADER_WRITER, readOnly );
        instantiateTree( recoveryCleanupWorkCollector, headerWriter );
    }

    @Override
    protected void afterTreeInstantiation( GBPTree<StringIndexKey,NativeIndexValue> tree )
    {
        validator = new NativeIndexKeyLengthValidator<>( tree.keyValueSizeCap(), layout );
    }

    @Override
    public IndexReader newReader()
    {
        assertOpen();
        return new StringIndexReader( tree, layout, descriptor );
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        validator.validate( tuple[0] );
    }
}
