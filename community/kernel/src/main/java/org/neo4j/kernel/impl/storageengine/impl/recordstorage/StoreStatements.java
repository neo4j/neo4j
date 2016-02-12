/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.util.function.Supplier;

import org.neo4j.collection.pool.MarshlandPool;
import org.neo4j.function.Factory;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * {@link Supplier} of {@link StoreStatement} instances for {@link RecordStorageEngine}.
 * Internally uses pooling to reduce need for actual instantiation.
 */
public class StoreStatements implements Supplier<StorageStatement>, AutoCloseable
{
    private final NeoStores neoStores;
    private final LockService lockService;
    private final Supplier<IndexReaderFactory> indexReaderFactory;
    private final Supplier<LabelScanReader> labelScanReader;
    private final Factory<StoreStatement> factory = new Factory<StoreStatement>()
    {
        @Override
        public StoreStatement newInstance()
        {
            return new StoreStatement( neoStores, lockService, indexReaderFactory, labelScanReader, pool );
        }
    };
    private final MarshlandPool<StoreStatement> pool = new MarshlandPool<>( factory );

    public StoreStatements( NeoStores neoStores, LockService lockService,
            Supplier<IndexReaderFactory> indexReaderFactory, Supplier<LabelScanReader> labelScanReader )
    {
        this.neoStores = neoStores;
        this.lockService = lockService;
        this.indexReaderFactory = indexReaderFactory;
        this.labelScanReader = labelScanReader;
    }

    @Override
    public StorageStatement get()
    {
        return pool.acquire().initialize();
    }

    @Override
    public void close()
    {
        pool.close();
    }
}
