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
package org.neo4j.consistency.checking.full;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_direct_io;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;

@TestDirectoryExtension
public class FullCheckFulltextIndexEmptyDocsIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    /**
     * Fulltext index created before 4.3.0-drop02 can contain empty documents added when there were nodes matching the
     * schema but having non-text values. This zip contains a couple of fulltext indexes with some empty documents.
     * Consistency checker should ignore such documents since we don't want to force rebuild of all fulltext indexes
     * when they actually are usable.
     */
    @Test
    void shouldNotReportEmptyDocsInFulltextIndexAsInconsistencies() throws Throwable {
        Config config = Config.newBuilder()
                .set(neo4j_home, testDirectory.homePath())
                .set(pagecache_direct_io, false)
                .build();

        DatabaseManagementService managementService = startUpOldDb(config);
        GraphDatabaseAPI db =
                (GraphDatabaseAPI) managementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        DatabaseLayout layout = db.databaseLayout();
        managementService.shutdown();

        ConsistencyCheckService.Result result = check(config, layout);
        assertTrue(result.isSuccessful());
    }

    private ConsistencyCheckService.Result check(Config config, DatabaseLayout layout) throws Exception {
        JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fs,
                config,
                NULL,
                NullLog.getInstance(),
                jobScheduler,
                Clocks.nanoClock(),
                new MemoryPools(config.get(memory_tracking)));
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();
        ConsistencyCheckService.Result result;
        try {
            result = new ConsistencyCheckService(layout)
                    .with(config)
                    .with(fs)
                    .with(pageCache)
                    .with(layout.databaseDirectory())
                    .runFullConsistencyCheck();
        } finally {
            pageCache.close();
            jobScheduler.close();
        }
        return result;
    }

    private DatabaseManagementService startUpOldDb(Config config) throws IOException {
        Unzip.unzip(getClass(), "SF5.0.0_fulltextWithEmptyDocs.zip", testDirectory.homePath());

        return new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(config)
                .build();
    }
}
