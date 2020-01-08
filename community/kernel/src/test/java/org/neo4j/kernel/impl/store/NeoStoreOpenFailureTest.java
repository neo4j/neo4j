/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.fail;
import static org.neo4j.test.AssumptionHelper.withoutReadPermissions;
import static org.neo4j.test.AssumptionHelper.withoutWritePermissions;

public class NeoStoreOpenFailureTest
{
    @Rule
    public PageCacheAndDependenciesRule rules = new PageCacheAndDependenciesRule().with( new DefaultFileSystemRule() );

    @Test
    public void mustCloseAllStoresIfNeoStoresFailToOpen() throws IOException
    {
        PageCache pageCache = rules.pageCache();
        DatabaseLayout databaseLayout = rules.directory().databaseLayout();
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
                databaseLayout, config, idGenFactory, pageCache, logProvider, fs, versions, formats, create, storeTypes,
                openOptions );
        File schemaStore = neoStores.getSchemaStore().getStorageFile();
        neoStores.close();

        // Make the schema store inaccessible, to sabotage the next initialisation we'll do.
        try ( Closeable ignored = withoutReadPermissions( schemaStore );
                Closeable ignored2 = withoutWritePermissions( schemaStore ) )
        {
            try
            {
                // This should fail due to the permissions we changed above.
                // And when it fails, the already-opened stores should be closed.
                new NeoStores( databaseLayout, config, idGenFactory, pageCache, logProvider, fs, versions, formats, create,
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
}
