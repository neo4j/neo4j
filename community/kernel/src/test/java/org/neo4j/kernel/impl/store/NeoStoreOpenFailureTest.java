/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.OpenOption;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
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
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@PageCacheExtension
class NeoStoreOpenFailureTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    @Test
    void mustCloseAllStoresIfNeoStoresFailToOpen()
    {
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        Config config = Config.defaults();
        IdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory( fileSystem );
        LogProvider logProvider = NullLogProvider.getInstance();
        VersionContextSupplier versions = EmptyVersionContextSupplier.EMPTY;
        RecordFormats formats = Standard.LATEST_RECORD_FORMATS;
        RecordFormatPropertyConfigurator.configureRecordFormat( formats, config );
        boolean create = true;
        StoreType[] storeTypes = StoreType.values();
        OpenOption[] openOptions = new OpenOption[0];
        NeoStores neoStores = new NeoStores(
                databaseLayout, config, idGenFactory, pageCache, logProvider, fileSystem, versions, formats, create, storeTypes,
                openOptions );
        File schemaStore = neoStores.getSchemaStore().getStorageFile();
        neoStores.close();

        // Make the schema store inaccessible, to sabotage the next initialisation we'll do.
        assumeTrue( schemaStore.setReadable( false ) );
        assumeTrue( schemaStore.setWritable( false ) );

        assertThrows( RuntimeException.class, () ->
                // This should fail due to the permissions we changed above.
                // And when it fails, the already-opened stores should be closed.
                new NeoStores( databaseLayout, config, idGenFactory, pageCache, logProvider, fileSystem, versions, formats, create, storeTypes, openOptions ) );

        // We verify that the successfully opened stores were closed again by the failed NeoStores open,
        // by closing the page cache, which will throw if not all files have been unmapped.
        pageCache.close();
    }
}
