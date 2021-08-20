/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.storageengine.api;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.Database;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@ImpermanentDbmsExtension
class StoreVersionCheckIT
{
    @Inject
    Database db;
    @Inject
    PageCache pc;
    @Inject
    FileSystemAbstraction fs;

    @Test
    void shouldBeAbleToFetchStorageVersionForOnlineDatabase()
    {
        //Given
        StoreVersion storeVersion = db.getStorageEngineFactory().versionInformation( db.getStoreId() );
        //Then
        assertThat( storeVersion ).isNotNull();
    }

    @Test
    void shouldBeAbleToFetchStorageVersionForOfflineDatabase() throws Exception
    {
        //Given
        DatabaseLayout databaseLayout = db.getDatabaseLayout();
        Config config = db.getConfig();

        //When
        db.shutdown();

        //Then
        StorageEngineFactory sef = StorageEngineFactory.selectStorageEngine( fs, databaseLayout, pc ).get();
        StoreVersionCheck storeVersionCheck = sef.versionCheck( fs, databaseLayout, config, pc, NullLogService.getInstance(), PageCacheTracer.NULL );
        String storeVersionStr = storeVersionCheck.storeVersion( CursorContext.NULL ).get();
        StoreVersion storeVersion = storeVersionCheck.versionInformation( storeVersionStr );
        assertThat( storeVersion ).isNotNull();
    }
}
