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
package org.neo4j.kernel.impl.store;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.file.OpenOption;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class NeoStoreOpenFailureTest
{
    @Rule
    public PageCacheAndDependenciesRule rules = new PageCacheAndDependenciesRule( DefaultFileSystemRule::new, null );

    @Test
    public void mustCloseAllStoresIfNeoStoresFailToOpen()
    {
        PageCache pageCache = rules.pageCache();
        File dir = rules.directory().directory( "dir" );
        File neoStoreFile = new File( dir, MetaDataStore.DEFAULT_NAME );
        Config config = Config.defaults();
        FileSystemAbstraction fs = rules.fileSystem();
        IdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory( fs );
        LogProvider logProvider = NullLogProvider.getInstance();
        VersionContextSupplier versions = EmptyVersionContextSupplier.EMPTY;
        RecordFormats formats = Standard.LATEST_RECORD_FORMATS;
        new RecordFormatPropertyConfigurator( formats, config ).configure();
        boolean create = true;
        StoreType[] storeTypes = StoreType.values();
        OpenOption[] openOptions = new OpenOption[0];
        NeoStores neoStores = new NeoStores(
                neoStoreFile, config, idGenFactory, pageCache, logProvider, fs, versions, formats, create, storeTypes,
                openOptions );
        File schemaStore = neoStores.getSchemaStore().getStorageFileName();
        neoStores.close();

        // Make the schema store inaccessible, to sabotage the next initialisation we'll do.
        assumeTrue( schemaStore.setReadable( false ) );
        assumeTrue( schemaStore.setWritable( false ) );

        try
        {
            // This should fail due to the permissions we changed above.
            // And when it fails, the already-opened stores should be closed.
            new NeoStores( neoStoreFile, config, idGenFactory, pageCache, logProvider, fs, versions, formats, create,
                    storeTypes, openOptions );
            fail( "Opening NeoStores should have thrown." );
        }
        catch ( RuntimeException ignore )
        {
        }

        // We verify that the successfully opened stores were closed again by the failed NeoStores open,
        // by closing the page cache, which will throw if not all files have been unmapped.
        pageCache.close();
    }
}
