/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.format.FormatOverrideMigrator;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class DbFormatIT {
    @Inject
    TestDirectory directory;

    @Test
    void defaultCommunityFormat() throws IOException {
        // The default format for community is aligned, but our test format migrator can be in play and override it.
        String property = System.getProperty(FormatOverrideMigrator.OVERRIDE_STORE_FORMAT_KEY);
        String expectedFormat = property != null ? property : PageAligned.LATEST_NAME;
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder(directory.homePath()).build();
        try {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            verifyStoreFormat(db, expectedFormat);
        } finally {
            dbms.shutdown();
        }
    }

    @Test
    void setFormatIsRespected() throws IOException {
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .setConfig(db_format, Standard.LATEST_NAME)
                .build();
        try {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
            verifyStoreFormat(db, Standard.LATEST_NAME);
        } finally {
            dbms.shutdown();
        }
    }

    protected void verifyStoreFormat(GraphDatabaseAPI db, String expectedFormat) throws IOException {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        PageCache pageCache = dependencyResolver.resolveDependency(PageCache.class);
        StorageEngineFactory storageEngineFactory = dependencyResolver.resolveDependency(StorageEngineFactory.class);
        var storeId = storageEngineFactory.retrieveStoreId(
                directory.getFileSystem(), db.databaseLayout(), pageCache, CursorContext.NULL_CONTEXT);
        assertEquals(expectedFormat, storeId.getFormatName());
    }
}
