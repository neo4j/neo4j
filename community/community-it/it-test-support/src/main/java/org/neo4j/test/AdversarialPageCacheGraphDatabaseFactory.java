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
package org.neo4j.test;

import java.nio.file.Path;
import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.pagecache.AdversarialPageCache;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

public final class AdversarialPageCacheGraphDatabaseFactory {
    private AdversarialPageCacheGraphDatabaseFactory() {
        throw new AssertionError("Not for instantiation!");
    }

    public static TestDatabaseManagementServiceBuilder create(
            Path homeDir, FileSystemAbstraction fs, Adversary adversary) {
        return new TestDatabaseManagementServiceBuilder(homeDir) {
            @Override
            protected DatabaseManagementService newDatabaseManagementService(
                    Config config, ExternalDependencies dependencies) {

                return new DatabaseManagementServiceFactory(DbmsInfo.COMMUNITY, CommunityEditionModule::new) {

                    @Override
                    protected GlobalModule createGlobalModule(
                            Config config, boolean daemonMode, ExternalDependencies dependencies) {
                        return new GlobalModule(config, dbmsInfo, daemonMode, dependencies) {
                            @Override
                            protected FileSystemAbstraction createFileSystemAbstraction() {
                                return fs;
                            }

                            @Override
                            protected PageCache createPageCache(
                                    FileSystemAbstraction fileSystem,
                                    Config config,
                                    LogService logging,
                                    Tracers tracers,
                                    JobScheduler jobScheduler,
                                    SystemNanoClock clock,
                                    MemoryPools memoryPools) {
                                PageCache pageCache = super.createPageCache(
                                        fileSystem, config, logging, tracers, jobScheduler, clock, memoryPools);
                                return new AdversarialPageCache(pageCache, adversary);
                            }
                        };
                    }
                }.build(config, daemonMode, dependencies);
            }
        };
    }
}
