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
package org.neo4j.internal.recordstorage;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.ReadOnlyIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageReader;

public class ReadableRecordStorageEngine extends LifecycleAdapter implements ReadableStorageEngine
{
    private final NeoStores neoStores;

    ReadableRecordStorageEngine( DatabaseLayout databaseLayout, Config config, PageCache pageCache, FileSystemAbstraction fs, LogProvider logProvider )
    {
        StoreFactory storeFactory =
                new StoreFactory( databaseLayout, config, new ReadOnlyIdGeneratorFactory( fs ), pageCache, fs, logProvider );
        neoStores = storeFactory.openAllNeoStores();
    }

    @Override
    public void start() throws Exception
    {
        neoStores.verifyStoreOk();
    }

    @Override
    public StorageReader newReader()
    {
        return new RecordStorageReader( neoStores );
    }

    @Override
    public void shutdown()
    {
        neoStores.close();
    }
}
